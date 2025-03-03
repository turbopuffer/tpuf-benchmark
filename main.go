package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"slices"
	"sync"
	"text/template"
	"time"

	"github.com/schollz/progressbar/v3"
	"golang.org/x/exp/rand"
	"golang.org/x/sync/errgroup"
	"gonum.org/v1/gonum/stat/distuv"
)

func main() {
	flag.Parse()

	rctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := run(rctx, cancel); err != nil {
		log.Fatalf("top-level error: %v", err)
	}
}

func run(ctx context.Context, shutdown context.CancelFunc) error {
	if *endpoint == "" {
		return errors.New("endpoint must be provided")
	} else if *apiKey == "" {
		return errors.New("api-key must be provided")
	}

	transport := http.Transport{
		// The idle connection timeout for AWS load balancers is 60s, but
		// Go's default is 90s. We need to turn this down to something that's
		// comfortably below the NLB timeout.
		IdleConnTimeout: 45 * time.Second,
	}
	if *allowTlsInsecure {
		transport.TLSClientConfig = &tls.Config {
			InsecureSkipVerify: true,
		}
	}

	httpClient := &http.Client{
		Transport: &transport,
	}

	// Script should be run via a cloud VM
	likelyCloudVM, err := likelyRunningOnCloudVM(ctx)
	if err != nil {
		log.Printf("failed to determine if running on cloud VM: %v", err)
	} else if !likelyCloudVM {
		log.Printf("detected that this script isn't running on a cloud VM")
		log.Printf("for best results, this benchmark needs to be run within the same region as the turbopuffer deployment")
	}

	// Load our template executor. Initially, we use a random vector source
	// for the sanity checks. Before upserting documents, we switch to a Cohere
	// vector source. Then, once we're done with setup, we switch back to a
	// random vector source (since we don't need to generate realistic documents
	// for queries and small upserts).
	executor := &TemplateExecutor{
		nextId:  0,
		vectors: RandomVectorSource(768),
		msmarco: &MSMarcoSource{},
	}

	// Parse all the query templates.
	queryTmpl, err := executor.ParseTemplate(
		ctx,
		"query",
		*queryTemplate,
	)
	if err != nil {
		return fmt.Errorf("parsing query template: %w", err)
	}
	docTmpl, err := executor.ParseTemplate(
		ctx,
		"document",
		*documentTemplate,
	)
	if err != nil {
		return fmt.Errorf("parsing document template: %w", err)
	}
	upsertTmpl, err := executor.ParseTemplate(
		ctx,
		"upsert",
		*upsertTemplate,
	)
	if err != nil {
		return fmt.Errorf("parsing upsert template: %w", err)
	}

	// Make sure we're able to do requests against the API
	log.Print("running sanity check against API")
	sanityNamespace := NewNamespace(
		ctx,
		httpClient,
		fmt.Sprintf("%s_sanity", *namespacePrefix),
		queryTmpl,
		docTmpl,
		upsertTmpl,
	)
	if err := runSanity(ctx, sanityNamespace); err != nil {
		return fmt.Errorf("failed sanity check: %w", err)
	}
	log.Print("sanity check passed")

	// Setup namespaces
	executor.vectors = NewCohereVectorSource()
	namespaces, sizes, err := setupNamespaces(
		ctx,
		httpClient,
		executor,
		queryTmpl,
		docTmpl,
		upsertTmpl,
	)
	if err != nil {
		return fmt.Errorf("failed to setup namespaces: %w", err)
	}
	executor.vectors = RandomVectorSource(768)

	// Wait until the largest namespace has been fully indexed,
	// i.e. we just dumped in a huge amount of documents
	if *benchmarkWaitForIndexing {
		if err := waitForIndexing(ctx, namespaces...); err != nil {
			return fmt.Errorf("failed to wait for indexing: %w", err)
		}
	}

	if *benchmarkPurgeCache {
		log.Println("purging caches before starting benchmark...")
		if err := purgeCache(ctx, namespaces...); err != nil {
			return fmt.Errorf("failed to purge cache: %w", err)
		}
		log.Println("caches purged")
	}

	log.Printf("starting benchmark, running for %s", *benchmarkDuration)

	// Generate query load
	queryLoad, err := generateQueryLoad(ctx, sizes)
	if err != nil {
		return fmt.Errorf("failed to generate query load: %w", err)
	}

	// Generate upsert load
	upsertLoad, err := generateUpsertLoad(ctx, len(namespaces))
	if err != nil {
		return fmt.Errorf("failed to generate upsert load: %w", err)
	}

	// Shutdown the benchmark automatically after the specified duration
	duration := *benchmarkDuration
	if duration > 0 {
		go func() {
			<-time.After(duration)
			log.Printf("benchmark duration of %s has elapsed, shutting down", duration)
			shutdown()
		}()
	}

	// Start up the reporter, i.e. to log the results of the benchmark
	// to the console periodically and write output files.
	reporter, err := StartReporter()
	if err != nil {
		return fmt.Errorf("failed to start reporter: %w", err)
	}
	defer func() {
		if err := reporter.Stop(); err != nil {
			log.Printf("failed to stop reporter: %v", err)
		}
	}()

	// Core benchmark loop
outer:
	for {
		select {
		case <-ctx.Done():
			break outer
		case idx := <-queryLoad:
			ns, size := namespaces[idx], sizes[idx]
			go func() {
				serverTimings, clientTime, err := ns.Query(ctx)
				if err != nil {
					if !errors.Is(err, context.Canceled) {
						log.Printf("error querying namespace %s: %v", ns.name, err)
					}
					return
				} else if serverTimings != nil {
					reporter.ReportQuery(
						ns.name,
						size,
						clientTime,
						serverTimings,
					)
				}
			}()
		case idx := <-upsertLoad:
			ns := namespaces[idx.NamespaceIndex]
			go func() {
				took, totalBytes, err := ns.Upsert(ctx, idx.NumDocs)
				if err != nil {
					if !errors.Is(err, context.Canceled) {
						log.Printf("error upserting documents to namespace %s: %v", ns.name, err)
					}
					return
				}
				reporter.ReportUpsert(
					ns.name,
					idx.NumDocs,
					totalBytes,
					took,
				)
			}()
		}
	}

	return nil
}

// Runs a sanity check against the turbopuffer API to make sure
// that the API is up and running, and that we're able to do requests
// against it.
func runSanity(ctx context.Context, ns *Namespace) error {
	if err := ns.Clear(ctx); err != nil {
		return fmt.Errorf("deleting existing documents: %w", err)
	}

	if _, _, err := ns.Upsert(ctx, 10); err != nil {
		return fmt.Errorf("upserting documents: %w", err)
	}

	serverTiming, clientDuration, err := ns.Query(ctx)
	if err != nil {
		return fmt.Errorf("querying namespace: %w", err)
	}

	// Little helper to detect discrepancies between client and server query latency
	// i.e. if >10ms, probably running in different regions
	var (
		serverMs = int64(*serverTiming.ProcessingTimeMs)
		clientMs = clientDuration.Milliseconds()
	)
	if serverMs+10 < clientMs {
		discrepancy := clientMs - serverMs
		log.Printf(
			"detected %d ms discrepancy between client and server query latency",
			discrepancy,
		)
		log.Println("are you running this script in the same region as turbopuffer?")
	}

	return nil
}

// This endpoint is common with most cloud providers, aka should work on GCP, AWS, Azure, etc.
// We use this to determine if we are running on a cloud VM, and log a warning if we aren't.
const metadataUrl = "169.254.169.254"

func likelyRunningOnCloudVM(ctx context.Context) (bool, error) {
	timedCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(timedCtx, "GET", "http://"+metadataUrl, nil)
	if err != nil {
		return false, fmt.Errorf("new request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return false, nil
		}
		return false, fmt.Errorf("do request: %w", err)
	}

	return resp.StatusCode == http.StatusOK, nil
}

// Configures all the namespaces we'll be benchmarking with and
// pre-populates them with data according to the provided flags.
//
// Returns the namespaces themselves and their associated sizes.
func setupNamespaces(
	ctx context.Context,
	client *http.Client,
	executor *TemplateExecutor,
	queryTmpl, docTmpl, upsertTmpl *template.Template,
) ([]*Namespace, []int, error) {
	if *namespaceCount == 0 {
		return nil, nil, errors.New("namespace count must be greater than 0")
	}

	// For setup, we use a template executor configured with a Cohere
	// vector source. This is used to generate realistic documents for
	// the namespaces. Once we're done with setup, we can use a simpler
	// vector source (i.e. random).

	defer func() {
		executor.lock.Lock()
		executor.vectors = RandomVectorSource(768)
		executor.lock.Unlock()
	}()

	// Load all the namespace objects
	namespaces := make([]*Namespace, *namespaceCount)
	for i := 0; i < *namespaceCount; i++ {
		namespaces[i] = NewNamespace(
			ctx,
			client,
			fmt.Sprintf("%s_%d", *namespacePrefix, i),
			queryTmpl,
			docTmpl,
			upsertTmpl,
		)
	}

	// Generate sizes for each namespace
	sizes := make([]int, *namespaceCount)
	switch *namespaceSizeDistribution {
	case "uniform":
		log.Printf("using uniform size distribution for namespaces")
		eachSize := *namespaceCombinedSize / int64(*namespaceCount)
		log.Printf("%d documents per namespace", eachSize)
		for i := range sizes {
			sizes[i] = int(eachSize)
		}
	case "lognormal":
		log.Printf("using lognormal size distribution for namespaces")
		log.Printf("mu: %.3f, sigma: %.3f", *logNormalMu, *logNormalSigma)
		sizes = generateLognormalSizes(
			*namespaceCount,
			*namespaceCombinedSize,
			*logNormalMu,
			*logNormalSigma,
		)
		printSizeDistributionOverview(sizes)
	default:
		return nil, nil, fmt.Errorf(
			"unsupported namespace size distribution: %s",
			*namespaceSizeDistribution,
		)
	}

	// Get the existing sizes of the namespaces.
	// If the namespace doesn't exist, the size will be 0.
	var (
		existingSizes = make([]int, *namespaceCount)
		bar           = progressbar.Default(int64(*namespaceCount), "syncing namespaces")
		eg            = new(errgroup.Group)
	)
	eg.SetLimit(max(1, runtime.GOMAXPROCS(0)*2))
	for i, ns := range namespaces {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		default:
		}
		eg.Go(func() error {
			size, err := ns.CurrentSize(ctx)
			if err != nil {
				return fmt.Errorf("getting current size: %w", err)
			}
			if size > int64(math.MaxInt) {
				return fmt.Errorf("namespace size too large: %d", size)
			}
			existingSizes[i] = int(size)
			bar.Add(1)
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, nil, fmt.Errorf("getting existing sizes: %w", err)
	}

	// Want to be careful about overwriting existing data
	// if the user didn't explicitly ask for it.
	var totalExisting int64
	for _, s := range existingSizes {
		totalExisting += int64(s)
	}
	if totalExisting > 0 {
		log.Printf("found %d existing documents in namespaces", totalExisting)
		log.Printf("would you like to delete them before proceeding? (yes/no/cancel)")
		log.Printf(
			"note: saying 'no' will skip the setup phase entirely and proceed with the benchmark",
		)
		var response string
		if _, err := fmt.Scanln(&response); err != nil {
			return nil, nil, fmt.Errorf("reading response: %w", err)
		}
		switch response {
		case "yes", "y":
			bar = progressbar.Default(int64(*namespaceCount), "clearing namespaces")
			for _, ns := range namespaces {
				select {
				case <-ctx.Done():
					return nil, nil, ctx.Err()
				default:
				}
				eg.Go(func() error {
					if err := ns.Clear(ctx); err != nil {
						return fmt.Errorf("clearing namespace: %w", err)
					}
					bar.Add(1)
					return nil
				})
			}
			if err := eg.Wait(); err != nil {
				return nil, nil, fmt.Errorf("clearing namespaces: %w", err)
			}
		case "no", "n":
			log.Printf("skipping setup phase, proceeding with benchmark")
			return namespaces, existingSizes, nil
		case "cancel", "c":
			log.Printf("bye")
			os.Exit(0)
		}
	}

	// Now, we need to upsert documents into the namespaces.
	// We have specialty logic here to make this phase go as fast
	// as possible, since we're likely upserting a *ton* of documents.
	if err := UpsertDocumentsToNamespaces(ctx, docTmpl, upsertTmpl, namespaces, sizes); err != nil {
		return nil, nil, fmt.Errorf("upserting documents to namespaces: %w", err)
	}

	return namespaces, sizes, nil
}

// Generates a sorted list of namespace sizes, totalling `total` documents
// across `n` namespaces. The sizes are generated using a lognormal distribution
// with the provided `mu` and `sigma` parameters.
func generateLognormalSizes(n int, total int64, mu, sigma float64) []int {
	ln := distuv.LogNormal{
		Mu:    mu,
		Sigma: sigma,
		Src:   rand.NewSource(42),
	}

	var (
		samples      = make([]float64, n)
		totalSamples float64
	)
	for i := range samples {
		s := ln.Rand()
		samples[i] = s
		totalSamples += s
	}
	slices.Sort(samples)

	sizes := make([]int, n)
	for i := range sizes {
		sizes[i] = int(float64(total) * (samples[i] / totalSamples))
	}

	return sizes
}

// Array of sizes should be sorted
func printSizeDistributionOverview(sizes []int) {
	percentile := func(p float64) int {
		return sizes[int(float64(len(sizes))*p)]
	}

	log.Printf("namespace size distribution (across %d namespaces):", len(sizes))
	log.Printf("min: %d", sizes[0])
	log.Printf("25th percentile: %d", percentile(0.25))
	log.Printf("50th percentile: %d", percentile(0.5))
	log.Printf("75th percentile: %d", percentile(0.75))
	log.Printf("90th percentile: %d", percentile(0.9))
	log.Printf("95th percentile: %d", percentile(0.95))
	log.Printf("99th percentile: %d", percentile(0.99))
	log.Printf("99.9th percentile: %d", percentile(0.999))
	log.Printf("max: %d", sizes[len(sizes)-1])

	var sum int64
	for _, s := range sizes {
		sum += int64(s)
	}
	log.Printf("total documents across all namespaces: %d", sum)
}

const indexedExhaustiveCountThreshold = 70_000

// Waits for a set of namespaces to be indexed. This is useful after
// we've upserted a large number of documents into a namespace, and we
// want to wait until the namespace is fully indexed before starting
// the benchmark.
func waitForIndexing(ctx context.Context, namespaces ...*Namespace) error {
	if len(namespaces) == 0 {
		return nil
	}

	remaining := map[*Namespace]struct{}{}
	for _, ns := range namespaces {
		remaining[ns] = struct{}{}
	}
	var lock sync.Mutex

	log.Printf(
		"waiting for %d namespace(s) to be indexed before starting benchmark",
		len(namespaces),
	)

	eg := new(errgroup.Group)
	eg.SetLimit(100)
	for {
		keys := make([]*Namespace, 0, len(remaining))
		for ns := range remaining {
			keys = append(keys, ns)
		}
		for _, ns := range keys {
			eg.Go(func() error {
				stats, _, err := ns.Query(ctx)
				if err != nil {
					return fmt.Errorf("querying namespace: %w", err)
				}
				var exhaustiveCount int64
				if stats != nil && stats.ExhaustiveCount != nil {
					exhaustiveCount = *stats.ExhaustiveCount
				}
				if exhaustiveCount < indexedExhaustiveCountThreshold {
					lock.Lock()
					delete(remaining, ns)
					lock.Unlock()
				}
				return nil
			})
		}
		if err := eg.Wait(); err != nil {
			return fmt.Errorf("waiting for namespaces to be indexed: %w", err)
		}
		if len(remaining) == 0 {
			break
		}
		log.Printf("%d namespace(s) still indexing, waiting 10s...", len(remaining))
		time.Sleep(time.Second * 10)
	}

	log.Println("all namespaces have been (reasonably) indexed")

	return nil
}

// Purges the cache of all the given namespaces.
// Used to ensure that the benchmark is as fair as possible, i.e. always starting
// off from a cold cache and having it warm up over time.
func purgeCache(ctx context.Context, namespaces ...*Namespace) error {
	eg := new(errgroup.Group)
	eg.SetLimit(100)
	for _, ns := range namespaces {
		eg.Go(func() error {
			if err := ns.PurgeCache(ctx); err != nil {
				return fmt.Errorf("purging cache: %w", err)
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("purging cache: %w", err)
	}
	return nil
}

// Generates the query load for the benchmark across a set of `n` namespaces.
// Distribution-dependent.
func generateQueryLoad(ctx context.Context, sizes []int) (<-chan int, error) {
	queries := make(chan int)

	// If the QPS is 0, never send any queries
	qps := *benchmarkQueriesPerSecond
	if qps <= 0 {
		go func() {
			<-ctx.Done()
			close(queries)
		}()
		return queries, nil
	}

	// Randomize the order of the namespaces, i.e.
	// decorrelate the query distribution from the size of the namespace
	indexes := make([]int, 0, len(sizes))
	for i := range sizes {
		if sizes[i] == 0 {
			continue // Don't query zero-sized namespaces
		}
		indexes = append(indexes, i)
	}
	rand.Shuffle(len(indexes), func(i, j int) {
		indexes[i], indexes[j] = indexes[j], indexes[i]
	})

	// It's common that only a subset of the namespaces are active at
	// a given time. Simulate this here by trimming set of namespaces.
	if *benchmarkActiveNamespacePct < 0 || *benchmarkActiveNamespacePct > 1 {
		return nil, errors.New("namespace-active-pct must be between 0 and 1")
	}
	activeNamespaces := int(float64(len(indexes)) * *benchmarkActiveNamespacePct)
	indexes = indexes[:activeNamespaces]

	// Build a query distribution which'll determine how queries are
	// distributed across the namespaces.
	var queryDistribution QueryDistribution
	switch *benchmarkQueryDistribution {
	case "uniform":
		queryDistribution = NewUniformQueryDistribution(len(indexes))
	case "pareto":
		alpha := *benchmarkQueryParetoAlpha
		if alpha < 0 {
			return nil, errors.New("query-pareto-alpha must be greater than 0")
		}
		queryDistribution = NewParetoQueryDistribution(len(indexes), alpha)
	default:
		return nil, fmt.Errorf("unsupported query distribution: %s", *benchmarkQueryDistribution)
	}

	// Start generating query load in the background
	interval := time.Duration(float64(time.Second) * (1 / qps))
	go func() {
		tkr := time.NewTicker(interval)
		defer tkr.Stop()
		for {
			select {
			case <-ctx.Done():
				close(queries)
				return
			case <-tkr.C:
				queries <- indexes[queryDistribution.NextIndex()]
			}
		}
	}()

	log.Printf("executing queries at %.2f QPS (%s distribution)", qps, *benchmarkQueryDistribution)

	return queries, nil
}

// Helper type to represent a pending upsert request.
type UpsertLoad struct {
	NamespaceIndex int
	NumDocs        int
}

// Generates upsert load for the benchmark across a set of `n` namespaces.
func generateUpsertLoad(ctx context.Context, n int) (<-chan UpsertLoad, error) {
	upserts := make(chan UpsertLoad)

	upsertSize := *upsertBatchSize
	if upsertSize <= 0 {
		return nil, errors.New("upsert-batch-size must be greater than 0")
	}

	// If the upserts per second is 0, never send any upserts
	if *benchmarkUpsertsPerSecond <= 0 {
		go func() {
			<-ctx.Done()
			close(upserts)
		}()
		return upserts, nil
	}

	// Randomize the order of the namespaces, i.e.
	// decorrelate the upsert distribution from the size of the namespace
	indexes := make([]int, n)
	for i := range indexes {
		indexes[i] = i
	}
	rand.Shuffle(len(indexes), func(i, j int) {
		indexes[i], indexes[j] = indexes[j], indexes[i]
	})

	// Every interval, increment the number of pending upserts.
	// If the number of pending upserts exceeds the minimum batch
	// size, send an upsert request to a random namespace.
	go func() {
		var (
			pendingUpserts int
			nextTarget     int
			tkr            = time.NewTicker(time.Second)
		)
		defer tkr.Stop()
		for {
			select {
			case <-ctx.Done():
				close(upserts)
				return
			case <-tkr.C:
				pendingUpserts += *benchmarkUpsertsPerSecond
				for pendingUpserts > upsertSize {
					upserts <- UpsertLoad{
						NamespaceIndex: indexes[nextTarget],
						NumDocs:        upsertSize,
					}
					pendingUpserts -= upsertSize
					nextTarget = (nextTarget + 1) % len(indexes)
				}
			}
		}
	}()

	log.Printf(
		"writing %d document(s) per second across all namespaces",
		*benchmarkUpsertsPerSecond,
	)
	log.Printf("upsert batch size: %d doc(s) per batch", upsertSize)

	return upserts, nil
}
