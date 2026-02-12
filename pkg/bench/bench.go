package bench

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"runtime"
	"slices"
	"sync"
	"time"

	"github.com/schollz/progressbar/v3"
	"github.com/turbopuffer/turbopuffer-go"
	"github.com/turbopuffer/turbopuffer-go/option"
	"golang.org/x/exp/rand"
	"golang.org/x/sync/errgroup"
	"gonum.org/v1/gonum/stat/distuv"
)

// ServiceConfig holds the configuration for accessing turbopuffer.
type ServiceConfig struct {
	APIKey           string
	Endpoint         string
	HostHeader       string
	AllowTLSInsecure bool
}

// NewClient creates a new turbopuffer client with the given configuration.
func (cfg *ServiceConfig) NewClient() turbopuffer.Client {
	transport := http.Transport{
		// The idle connection timeout for AWS load balancers is 60s, but
		// Go's default is 90s. We need to turn this down to something that's
		// comfortably below the NLB timeout.
		IdleConnTimeout: 45 * time.Second,
	}
	if cfg.AllowTLSInsecure {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	tpufOptions := []option.RequestOption{
		option.WithAPIKey(cfg.APIKey),
		option.WithBaseURL(cfg.Endpoint),
		option.WithHTTPClient(&http.Client{
			Transport: &transport,
		}),
	}
	if cfg.HostHeader != "" {
		tpufOptions = append(tpufOptions, option.WithHeader("Host", cfg.HostHeader))
	}
	return turbopuffer.NewClient(tpufOptions...)
}

// RunConfig holds all configuration for running an individual benchmark.
type RunConfig struct {
	// Template settings
	UpsertTemplate   string
	DocumentTemplate string
	QueryTemplate    string

	// Namespace settings
	NamespacePrefix              string
	NamespaceCount               int
	NamespaceCombinedSize        int64
	NamespaceSizeDistribution    string
	LogNormalMu                  float64
	LogNormalSigma               float64
	NamespaceSetupConcurrency    int
	NamespaceSetupConcurrencyMax int
	NamespaceSetupBatchSize      int

	// Benchmark settings
	PromptToClear      bool
	WaitForIndexing    bool
	PurgeCache         bool
	WarmCache          bool
	QueriesPerSecond   float64
	QueryConcurrency   int
	QueryDistribution  string
	QueryParetoAlpha   float64
	ActiveNamespacePct float64
	UpsertsPerSecond   int
	UpsertConcurrency  int
	UpsertBatchSize    int
	Duration           time.Duration
	QueryRetries       int
	OutputDir          string
}

func Run(ctx context.Context, shutdown context.CancelFunc, client *turbopuffer.Client, tmpls *Templates, cfg RunConfig) error {

	log.Print("sanity check passed")

	// Setup namespaces
	tmpls.exec.SetVectorSource(NewCohereVectorSource())
	namespaces, sizes, err := setupNamespaces(ctx, client, tmpls, cfg)
	if err != nil {
		return fmt.Errorf("failed to setup namespaces: %w", err)
	}
	tmpls.exec.SetVectorSource(RandomVectorSource(1024))

	// Wait until the largest namespace has been fully indexed,
	// i.e. we just dumped in a huge amount of documents
	if cfg.WaitForIndexing {
		if err := waitForIndexing(ctx, namespaces...); err != nil {
			return fmt.Errorf("failed to wait for indexing: %w", err)
		}
	}

	if cfg.PurgeCache {
		log.Println("purging caches before starting benchmark...")
		if err := purgeCache(ctx, namespaces...); err != nil {
			return fmt.Errorf("failed to purge cache: %w", err)
		}
		log.Println("caches purged")
	}

	if cfg.WarmCache {
		log.Println("warming caches before starting benchmark...")
		if err := warmCache(ctx, namespaces...); err != nil {
			return fmt.Errorf("failed to warm cache: %w", err)
		}
		log.Println("caches warmed")
	}

	log.Printf("starting benchmark, running for %s", cfg.Duration)

	// Generate query load
	queryLoad, err := generateQueryLoad(ctx, sizes, cfg)
	if err != nil {
		return fmt.Errorf("failed to generate query load: %w", err)
	}

	// Generate upsert load
	upsertLoad, err := generateUpsertLoad(ctx, len(namespaces), cfg)
	if err != nil {
		return fmt.Errorf("failed to generate upsert load: %w", err)
	}

	// Shutdown the benchmark automatically after the specified duration
	duration := cfg.Duration
	if duration > 0 {
		go func() {
			<-time.After(duration)
			log.Printf("benchmark duration of %s has elapsed, shutting down", duration)
			shutdown()
		}()
	}

	// Start up the reporter, i.e. to log the results of the benchmark
	// to the console periodically and write output files.
	reporter, err := StartReporter(cfg.OutputDir)
	if err != nil {
		return fmt.Errorf("failed to start reporter: %w", err)
	}
	defer func() {
		if err := reporter.Stop(); err != nil {
			log.Printf("failed to stop reporter: %v", err)
		}
	}()

	var wg sync.WaitGroup
	for range cfg.QueryConcurrency {
		wg.Go(func() {
			for {
				select {
				case <-ctx.Done():
					return
				case idx := <-queryLoad:
					ns, size := namespaces[idx], sizes[idx]
					performance, clientTime, err := ns.Query(ctx, cfg.QueryRetries)
					if err != nil {
						if !errors.Is(err, context.Canceled) {
							log.Printf("error querying namespace %s: %v", ns.ID(), err)
						}
						return
					} else if performance != nil {
						reporter.ReportQuery(
							ns.ID(),
							size,
							clientTime,
							performance,
						)
					}
				}
			}
		})
	}
	for range cfg.UpsertConcurrency {
		wg.Go(func() {
			for {
				select {
				case <-ctx.Done():
					return
				case idx := <-upsertLoad:
					ns := namespaces[idx.NamespaceIndex]
					took, totalBytes, err := ns.Upsert(ctx, idx.NumDocs)
					if err != nil {
						if !errors.Is(err, context.Canceled) {
							log.Printf("error upserting documents to namespace %s: %v", ns.ID(), err)
						}
						return
					}
					reporter.ReportUpsert(
						ns.ID(),
						idx.NumDocs,
						totalBytes,
						took,
					)
				}
			}
		})
	}
	wg.Wait()

	return nil
}

// purgeCache purges the cache of all the given namespaces.
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

// warmCache warms the cache of all the given namespaces.
// Used to ensure that the benchmark is as fair as possible, i.e. always starting
// off from a warm cache.
func warmCache(ctx context.Context, namespaces ...*Namespace) error {
	eg := new(errgroup.Group)
	eg.SetLimit(100)
	for _, ns := range namespaces {
		eg.Go(func() error {
			if err := ns.WarmCache(ctx); err != nil {
				return fmt.Errorf("warming cache: %w", err)
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("warming cache: %w", err)
	}
	return nil
}

// setupNamespaces configures all the namespaces we'll be benchmarking with and
// pre-populates them with data according to the provided flags.
//
// Returns the namespaces themselves and their associated sizes.
func setupNamespaces(
	ctx context.Context,
	client *turbopuffer.Client,
	tmpls *Templates,
	cfg RunConfig,
) ([]*Namespace, []int, error) {
	if cfg.NamespaceCount == 0 {
		return nil, nil, errors.New("namespace count must be greater than 0")
	}

	// For setup, we use a template executor configured with a Cohere
	// vector source. This is used to generate realistic documents for
	// the namespaces. Once we're done with setup, we can use a simpler
	// vector source (i.e. random).
	defer func() { tmpls.exec.SetVectorSource(RandomVectorSource(1024)) }()

	// Load all the namespace objects
	namespaces := make([]*Namespace, cfg.NamespaceCount)
	for i := range cfg.NamespaceCount {
		namespaces[i] = NewNamespace(
			ctx,
			client,
			fmt.Sprintf("%s_%d", cfg.NamespacePrefix, i),
			tmpls,
		)
	}

	// Generate sizes for each namespace
	sizes := make([]int, cfg.NamespaceCount)
	switch cfg.NamespaceSizeDistribution {
	case "uniform":
		log.Printf("using uniform size distribution for namespaces")
		eachSize := cfg.NamespaceCombinedSize / int64(cfg.NamespaceCount)
		log.Printf("%d documents per namespace", eachSize)
		for i := range sizes {
			sizes[i] = int(eachSize)
		}
	case "lognormal":
		log.Printf("using lognormal size distribution for namespaces")
		log.Printf("mu: %.3f, sigma: %.3f", cfg.LogNormalMu, cfg.LogNormalSigma)
		sizes = generateLognormalSizes(
			cfg.NamespaceCount,
			cfg.NamespaceCombinedSize,
			cfg.LogNormalMu,
			cfg.LogNormalSigma,
		)
		printSizeDistributionOverview(sizes)
	default:
		return nil, nil, fmt.Errorf(
			"unsupported namespace size distribution: %s",
			cfg.NamespaceSizeDistribution,
		)
	}

	// Get the existing sizes of the namespaces.
	// If the namespace doesn't exist, the size will be 0.
	var (
		existingSizes = make([]int, cfg.NamespaceCount)
		bar           = progressbar.Default(int64(cfg.NamespaceCount), "syncing namespaces")
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
		if !cfg.PromptToClear {
			log.Printf("skipping setup phase, proceeding with benchmark")
			return namespaces, existingSizes, nil
		}
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
			bar = progressbar.Default(int64(cfg.NamespaceCount), "clearing namespaces")
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
	if err := UpsertDocumentsToNamespaces(
		ctx, tmpls.Document, tmpls.Upsert, namespaces, sizes,
		cfg.NamespaceSetupConcurrency, cfg.NamespaceSetupConcurrencyMax, cfg.NamespaceSetupBatchSize,
	); err != nil {
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

// waitForIndexing waits for a set of namespaces to be indexed. This is useful
// after we've upserted a large number of documents into a namespace, and we
// want to wait until the namespace is fully indexed before starting the
// benchmark.
func waitForIndexing(ctx context.Context, namespaces ...*Namespace) error {
	const indexedExhaustiveCountThreshold = 70_000

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
				stats, _, err := ns.Query(ctx, 0)
				if err != nil {
					return fmt.Errorf("querying namespace: %w", err)
				}
				if stats.ExhaustiveSearchCount < indexedExhaustiveCountThreshold {
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

// generateQueryLoad generates the query load for the benchmark across a set of
// `n` namespaces.
// Distribution-dependent.
func generateQueryLoad(ctx context.Context, sizes []int, cfg RunConfig) (<-chan int, error) {
	queries := make(chan int)

	// If the QPS is 0, never send any queries
	qps := cfg.QueriesPerSecond
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
	if cfg.ActiveNamespacePct < 0 || cfg.ActiveNamespacePct > 1 {
		return nil, errors.New("namespace-active-pct must be between 0 and 1")
	}
	activeNamespaces := int(float64(len(indexes)) * cfg.ActiveNamespacePct)
	indexes = indexes[:activeNamespaces]

	// Build a query distribution which'll determine how queries are
	// distributed across the namespaces.
	var queryDistribution QueryDistribution
	switch cfg.QueryDistribution {
	case "uniform":
		queryDistribution = NewUniformQueryDistribution(len(indexes))
	case "round-robin":
		queryDistribution = NewRoundRobinQueryDistribution(len(indexes))
	case "pareto":
		alpha := cfg.QueryParetoAlpha
		if alpha < 0 {
			return nil, errors.New("query-pareto-alpha must be greater than 0")
		}
		queryDistribution = NewParetoQueryDistribution(len(indexes), alpha)
	default:
		return nil, fmt.Errorf("unsupported query distribution: %s", cfg.QueryDistribution)
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

	log.Printf("executing queries at %.2f QPS (%s distribution)", qps, cfg.QueryDistribution)

	return queries, nil
}

// UpsertLoad is a tuple of a namespace index and the number of documents to upsert.
type UpsertLoad struct {
	NamespaceIndex int
	NumDocs        int
}

// Generates upsert load for the benchmark across a set of `n` namespaces.
func generateUpsertLoad(ctx context.Context, n int, cfg RunConfig) (<-chan UpsertLoad, error) {
	upserts := make(chan UpsertLoad)

	upsertSize := cfg.UpsertBatchSize
	if upsertSize <= 0 {
		return nil, errors.New("upsert-batch-size must be greater than 0")
	}

	// If the upserts per second is 0, never send any upserts
	if cfg.UpsertsPerSecond <= 0 {
		go func() {
			<-ctx.Done()
			close(upserts)
		}()
		return upserts, nil
	}

	// Randomize the order of the namespaces, i.e.
	// decorrelate the upsert distribution from the size of the namespace
	indexes := rand.Perm(n)

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
				pendingUpserts += cfg.UpsertsPerSecond
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
		cfg.UpsertsPerSecond,
	)
	log.Printf("upsert batch size: %d doc(s) per batch", upsertSize)

	return upserts, nil
}
