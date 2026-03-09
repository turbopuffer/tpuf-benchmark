package bench

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/turbopuffer/tpuf-benchmark/pkg/output"
	"github.com/turbopuffer/turbopuffer-go"
	"github.com/turbopuffer/turbopuffer-go/option"
	"golang.org/x/exp/rand"
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
	transport := &http.Transport{
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
			Transport: transport,
		}),
	}
	if cfg.HostHeader != "" {
		tpufOptions = append(tpufOptions, option.WithHeader("Host", cfg.HostHeader))
	}
	return turbopuffer.NewClient(tpufOptions...)
}

// RuntimeConfig holds all user-provided runtime configuration for running a
// benchmark.
type RuntimeConfig struct {
	NamespacePrefix              string
	NamespaceSetupConcurrency    int
	NamespaceSetupConcurrencyMax int
	IfNonempty                   string
	OutputDir                    string
	WarmCache                    bool
	PurgeCache                   bool
	Duration                     time.Duration
}

func Run(
	ctx context.Context,
	client *turbopuffer.Client,
	def *Definition,
	cfg RuntimeConfig,
	logger *output.Logger,
) error {
	// Validate the output directory before doing any long-running work.
	if cfg.OutputDir != "" {
		if _, err := os.Stat(cfg.OutputDir); err == nil {
			return fmt.Errorf("output directory already exists: %s", cfg.OutputDir)
		}
	}
	if cfg.PurgeCache && (cfg.WarmCache || def.Setup.WarmCache) {
		return fmt.Errorf("cannot set both warm cache and purge cache")
	}

	logger.NextStage(output.StageSettingUpNamespaces)

	// Setup namespaces.
	ingestStart := time.Now()
	namespaces, sizes, err := func() (namespaces []*Namespace, sizes []int, err error) {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		return setupNamespaces(ctx, client, def, cfg, logger)
	}()
	if err != nil {
		return err
	}
	ingestDuration := time.Since(ingestStart)

	// Wait until all namespaces have been fully indexed.
	var indexedDuration time.Duration
	if def.Setup.WaitForIndexing {
		logger.NextStage(output.StageIndexing)
		task := logger.Task("indexing", len(namespaces))
		if err := waitForIndexing(ctx, task, namespaces...); err != nil {
			return fmt.Errorf("failed to wait for indexing: %w", err)
		}
		indexedDuration = time.Since(ingestStart)
	}

	// Log aggregate namespace stats.
	metadatas, err := forEachNamespace(ctx, namespaces,
		func(ctx context.Context, ns *Namespace) (*turbopuffer.NamespaceMetadata, error) {
			return ns.Metadata(ctx)
		})
	if err != nil {
		return err
	}
	var totalLogicalBytes, totalRowCount int64
	for _, m := range metadatas {
		totalLogicalBytes += m.ApproxLogicalBytes
		totalRowCount += m.ApproxRowCount
	}
	logger.Detailf("aggregate namespace stats: %s logical bytes, %s rows",
		humanize.Bytes(uint64(totalLogicalBytes)),
		humanize.Comma(totalRowCount))

	if cfg.PurgeCache {
		logger.NextStage(output.StagePurgingCache)
		logger.Detailf("purging caches before starting benchmark...")
		if err := purgeCache(ctx, namespaces...); err != nil {
			return fmt.Errorf("failed to purge cache: %w", err)
		}
	}

	if cfg.WarmCache || def.Setup.WarmCache {
		logger.NextStage(output.StageWarmingCache)
		logger.Detailf("warming caches before starting benchmark...")
		_, err := forEachNamespace(ctx, namespaces,
			func(ctx context.Context, ns *Namespace) (struct{}, error) {
				return struct{}{}, ns.WarmCache(ctx)
			})
		if err != nil {
			return fmt.Errorf("failed to warm cache: %w", err)
		}
	}

	if def.Setup.WaitForCacheHitRatio > 0 && len(def.Workloads.Query) > 0 {
		logger.NextStage(output.StageWarmingCache)
		if err := waitForWarmCache(ctx, logger, def.Setup.WaitForCacheHitRatio, def.Workloads.Query, namespaces...); err != nil {
			return err
		}
	}

	logger.NextStage(output.StageBenchmark)
	logger.Detailf("starting benchmark; will run for %s", def.Duration)
	benchTask := logger.Task("benchmark", int(def.Duration.Seconds()))

	// Shutdown the benchmark automatically after the specified duration.
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()
	if def.Duration > 0 {
		go func() {
			<-time.After(def.Duration)
			benchTask.Detailf("benchmark duration of %s has elapsed, shutting down", def.Duration)
			cancel()
		}()
	}

	// Start up the reporter, i.e. to log the results of the benchmark
	// to the console periodically and write output files.
	reporter, err := StartReporter(def, cfg.OutputDir, logger)
	if err != nil {
		return fmt.Errorf("failed to start reporter: %w", err)
	}
	reporter.SetIngestResult(totalLogicalBytes, ingestDuration, indexedDuration, def.Setup.WaitForIndexing)
	defer func() {
		if err := reporter.Stop(); err != nil {
			logger.Errorf("failed to stop reporter: %v", err)
		}
	}()

	var wg sync.WaitGroup
	for queryWorkloadName, queryWorkload := range def.Workloads.Query {
		// Generate query load.
		queryLoad, err := generateQueryLoad(ctx, sizes, queryWorkload, benchTask)
		if err != nil {
			return fmt.Errorf("failed to generate query load: %w", err)
		}
		for range queryWorkload.Concurrency {
			wg.Go(func() {
				for {
					select {
					case <-ctx.Done():
						return
					case idx := <-queryLoad:
						ns, size := namespaces[idx], sizes[idx]
						performance, clientTime, err := ns.Query(ctx, queryWorkload.MaxRetries, queryWorkload.QueryTemplate)
						if err != nil {
							if !errors.Is(err, context.Canceled) {
								logger.Errorf("error querying namespace %s: %v", ns.ID(), err)
							}
							return
						} else if performance != nil {
							reporter.ReportQuery(
								queryWorkloadName,
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
	}
	for upsertWorkloadName, upsertWorkload := range def.Workloads.Upsert {
		// Generate upsert load.
		upsertLoad, err := generateUpsertLoad(ctx, len(namespaces), upsertWorkload, benchTask)
		if err != nil {
			return fmt.Errorf("failed to generate upsert load: %w", err)
		}
		for range upsertWorkload.Concurrency {
			wg.Go(func() {
				for {
					select {
					case <-ctx.Done():
						return
					case idx := <-upsertLoad:
						ns := namespaces[idx.NamespaceIndex]
						took, totalBytes, err := ns.Upsert(ctx, idx.NumDocs, upsertWorkload.DocumentTemplate, upsertWorkload.UpsertTemplate)
						if err != nil {
							if !errors.Is(err, context.Canceled) {
								logger.Errorf("error upserting documents to namespace %s: %v", ns.ID(), err)
							}
							return
						}
						reporter.ReportUpsert(
							upsertWorkloadName,
							ns.ID(),
							idx.NumDocs,
							totalBytes,
							took,
						)
					}
				}
			})
		}
	}
	wg.Wait()

	logger.NextStage(output.StageDone)
	return nil
}

// purgeCache purges the cache of all the given namespaces.
// Used to ensure that the benchmark is as fair as possible, i.e. always starting
// off from a cold cache and having it warm up over time.
func purgeCache(ctx context.Context, namespaces ...*Namespace) error {
	_, err := forEachNamespace(ctx, namespaces,
		func(ctx context.Context, ns *Namespace) (struct{}, error) {
			return struct{}{}, ns.PurgeCache(ctx)
		})
	return err
}

// waitForWarmCache polls with workload queries until each workload reports a
// cache hit ratio >= minCacheHitRatio for consecutiveWarmQueries consecutive
// queries on every namespace.
func waitForWarmCache(ctx context.Context, logger *output.Logger, minCacheHitRatio float64, queryWorkloads map[string]*QueryWorkload, namespaces ...*Namespace) error {
	const consecutiveWarmQueries = 3
	logger.Detailf("waiting for cache hit ratio >= %.2f (%d consecutive queries per workload)...", minCacheHitRatio, consecutiveWarmQueries)
	_, err := forEachNamespace(ctx, namespaces,
		func(ctx context.Context, ns *Namespace) (struct{}, error) {
			const minBackoff = 128 * time.Millisecond
			const maxBackoff = 8 * time.Second

			// Track consecutive passes per workload.
			consecutive := make(map[string]int, len(queryWorkloads))
			for backoff := minBackoff; ; {
				allDone := true
				for name, workload := range queryWorkloads {
					if consecutive[name] >= consecutiveWarmQueries {
						continue
					}
					perf, _, err := ns.Query(ctx, 0, workload.QueryTemplate)
					if err != nil {
						return struct{}{}, err
					}
					if perf.CacheHitRatio >= minCacheHitRatio {
						consecutive[name]++
						logger.Detailf("namespace %s workload %s cache_hit_ratio=%.4f (%d/%d)",
							ns.ID(), name, perf.CacheHitRatio, consecutive[name], consecutiveWarmQueries)
						backoff = max(backoff/2, minBackoff)
					} else {
						logger.Detailf("namespace %s workload %s cache_hit_ratio=%.4f < %.2f; resetting and backing off for %s",
							ns.ID(), name, perf.CacheHitRatio, minCacheHitRatio, backoff)
						consecutive[name] = 0
						allDone = false
						backoff = min(backoff*2, maxBackoff)
						break
					}
				}
				if allDone {
					return struct{}{}, nil
				}
				select {
				case <-ctx.Done():
					return struct{}{}, ctx.Err()
				case <-time.After(backoff):
				}
			}
		})
	return err
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
func printSizeDistributionOverview(sizes []int, logger *output.Logger) {
	percentile := func(p float64) int {
		return sizes[int(float64(len(sizes))*p)]
	}

	logger.Detailf("namespace size distribution (across %d namespaces):", len(sizes))
	logger.Detailf("min: %d", sizes[0])
	logger.Detailf("25th percentile: %d", percentile(0.25))
	logger.Detailf("50th percentile: %d", percentile(0.5))
	logger.Detailf("75th percentile: %d", percentile(0.75))
	logger.Detailf("90th percentile: %d", percentile(0.9))
	logger.Detailf("95th percentile: %d", percentile(0.95))
	logger.Detailf("99th percentile: %d", percentile(0.99))
	logger.Detailf("99.9th percentile: %d", percentile(0.999))
	logger.Detailf("max: %d", sizes[len(sizes)-1])

	var sum int64
	for _, s := range sizes {
		sum += int64(s)
	}
	logger.Detailf("total documents across all namespaces: %d", sum)
}

// waitForIndexing waits for a set of namespaces to be indexed. This is useful
// after we've upserted a large number of documents into a namespace, and we
// want to wait until the namespace is fully indexed before starting the
// benchmark.
func waitForIndexing(ctx context.Context, task *output.TaskProgress, namespaces ...*Namespace) error {
	if len(namespaces) == 0 {
		return nil
	}

	pending := slices.Clone(namespaces)
	task.Detailf(
		"waiting for %d namespace(s) to be indexed before starting benchmark",
		len(namespaces),
	)

	for len(pending) > 0 {
		metadatas, err := forEachNamespace(ctx, pending,
			func(ctx context.Context, ns *Namespace) (*turbopuffer.NamespaceMetadata, error) {
				return ns.Metadata(ctx)
			})
		if err != nil {
			return fmt.Errorf("waiting for namespaces to be indexed: %w", err)
		}

		var unindexedBytes uint64
		stillPending := pending[:0]
		for i, m := range metadatas {
			if m.Index.Status != "up-to-date" {
				unindexedBytes += uint64(m.Index.UnindexedBytes)
				stillPending = append(stillPending, pending[i])
			}
		}
		pending = stillPending

		if len(pending) == 0 {
			break
		}
		task.Detailf("%d namespace(s) with %s unindexed data, waiting 10s...",
			len(pending), humanize.Bytes(unindexedBytes))
		task.SetProgression(len(namespaces)-len(pending), "%d namespace(s) with %s unindexed data still indexing",
			len(pending), humanize.Bytes(unindexedBytes))
		time.Sleep(10 * time.Second)
	}
	task.Detailf("all namespaces have been indexed")
	return nil
}

// generateQueryLoad generates the query load for the benchmark across a set of
// `n` namespaces.
// Distribution-dependent.
func generateQueryLoad(ctx context.Context, sizes []int, queryWorkload *QueryWorkload, task *output.TaskProgress) (<-chan int, error) {
	queries := make(chan int)

	// If the QPS is 0, never send any queries
	qps := queryWorkload.QPS
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
	if queryWorkload.ActiveNamespacePct < 0 || queryWorkload.ActiveNamespacePct > 1 {
		return nil, errors.New("namespace-active-pct must be between 0 and 1")
	}
	activeNamespaces := int(float64(len(indexes)) * queryWorkload.ActiveNamespacePct)
	indexes = indexes[:activeNamespaces]

	// Build a query distribution which'll determine how queries are
	// distributed across the namespaces.
	var queryDistribution QueryDistribution
	switch queryWorkload.QueryDistribution {
	case "uniform":
		queryDistribution = NewUniformQueryDistribution(len(indexes))
	case "round-robin":
		queryDistribution = NewRoundRobinQueryDistribution(len(indexes))
	case "pareto":
		alpha := queryWorkload.QueryParetoAlpha
		if alpha < 0 {
			return nil, errors.New("query-pareto-alpha must be greater than 0")
		}
		queryDistribution = NewParetoQueryDistribution(len(indexes), alpha)
	default:
		return nil, fmt.Errorf("unsupported query distribution: %s", queryWorkload.QueryDistribution)
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
	task.Detailf("executing queries at %.2f QPS (%s distribution)", qps, queryWorkload.QueryDistribution)
	return queries, nil
}

// UpsertLoad is a tuple of a namespace index and the number of documents to upsert.
type UpsertLoad struct {
	NamespaceIndex int
	NumDocs        int
}

// Generates upsert load for the benchmark across a set of `n` namespaces.
func generateUpsertLoad(ctx context.Context, n int, upsertWorkload *UpsertWorkload, task *output.TaskProgress) (<-chan UpsertLoad, error) {
	upserts := make(chan UpsertLoad)

	upsertSize := upsertWorkload.UpsertBatchSize
	if upsertSize <= 0 {
		return nil, errors.New("upsert-batch-size must be greater than 0")
	}

	// If the upserts per second is 0, never send any upserts
	if upsertWorkload.WPS <= 0 {
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
				pendingUpserts += int(upsertWorkload.WPS)
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
	task.Detailf("writing %d document(s) per second across all namespaces", int(upsertWorkload.WPS))
	task.Detailf("upsert batch size: %d doc(s) per batch", upsertWorkload.UpsertBatchSize)
	return upserts, nil
}
