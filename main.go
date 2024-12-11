package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/turbopuffer/tpuf-benchmark/turbopuffer"

	"github.com/schollz/progressbar/v3"
	"golang.org/x/sync/errgroup"
)

func main() {
	flag.Parse()

	logger := newLogger()

	rctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	var exitCode int
	if err := run(rctx, logger, cancel); err != nil {
		logger.Error("encountered top-level error", slog.String("error", err.Error()))
		exitCode = 1
	}

	os.Exit(exitCode)
}

func run(ctx context.Context, logger *slog.Logger, shutdown context.CancelFunc) error {
	_ = shutdown

	var missingRequiredFlags []string
	if *apiKey == "" {
		missingRequiredFlags = append(missingRequiredFlags, "api-key")
	}
	if *endpoint == "" {
		missingRequiredFlags = append(missingRequiredFlags, "endpoint")
	}
	if len(missingRequiredFlags) > 0 {
		logger.Error(
			"missing required flags",
			slog.Any("flags", missingRequiredFlags),
		)
		flag.Usage()
		return nil
	}

	if err := logWarningIfNotOnCloudVM(ctx, logger); err != nil {
		logger.Warn("failed to check if running on cloud VM", slog.String("error", err.Error()))
	}

	dataset, err := loadDataset(ctx, *namespaceSizeMax)
	if err != nil {
		return fmt.Errorf("loading dataset: %w", err)
	}

	client := turbopuffer.NewClient(*apiKey, turbopuffer.WithBaseURL(*endpoint))
	logger.Debug("initialized turbopuffer client", slog.String("endpoint", *endpoint))

	namespaces, err := loadNamespaces(ctx, client, *namespacePrefix, *namespaceCount)
	if err != nil {
		return fmt.Errorf("loading namespaces: %w", err)
	}

	var reporter *reporter
	if *reportInterval > 0 {
		reporter = newReporter(*reportInterval)
		go reporter.start(ctx)
	}

	runner := &benchmarkRunner{
		namespaces: namespaces,
		dataset:    dataset,
		reporter:   reporter,
	}

	steps := runner.plan(logger)
	if len(steps) == 0 {
		logger.Warn("no benchmark steps to run, maybe check your configuration?")
		return nil
	}

	for i, step := range steps {
		fmt.Printf("\nRunning benchmark step %d: %s\n\n", i+1, step.desc())
		if err := step.run(ctx, logger); err != nil {
			return fmt.Errorf("step %d: %w", i+1, err)
		}
	}

	return nil
}

type benchmarkRunner struct {
	namespaces []*namespace
	dataset    []turbopuffer.Document
	reporter   *reporter
}

func (br *benchmarkRunner) plan(logger *slog.Logger) []benchmarkStep {
	var steps []benchmarkStep

	if *overrideExisting {
		steps = append(steps, &benchmarkStepDelete{namespaces: br.namespaces})
	}

	if !br.anyExistingDocuments() || *overrideExisting {
		steps = append(steps, &benchmarkStepSetup{
			namespaces: br.namespaces,
			dataset:    br.dataset,
			sizes: generateSizesLognormal(
				len(br.namespaces),
				*namespaceSizeMin,
				*namespaceSizeMax,
				*setupLognormalMu,
				*setupLognormalSigma,
			),
		})
		steps = append(steps, &benchmarkStepWaitForIndexing{
			namespaces:          br.namespaces[len(br.namespaces)-1:],
			exhaustiveThreshold: 75_000, // Conservative, but should be enough
		})
	} else {
		logger.Info("found existing documents in namespaces, skipping initial upserts. if you want to override this existing data, pass -override-existing")
	}

	var queryStep *benchmarkStepQuery
	if *namespaceDistributedQps > 0 {
		var warmupPeriod time.Duration
		if *namespaceDistributedQps > 5 {
			const warmupPer5Qps = time.Second * 10
			warmupPeriod = time.Duration(*namespaceDistributedQps/5) * warmupPer5Qps
		}
		queryStep = &benchmarkStepQuery{
			namespaces:     br.namespaces,
			dataset:        br.dataset,
			distributedQps: *namespaceDistributedQps,
			reports:        br.reporter,
			warmupPeriod:   warmupPeriod,
		}
	}

	var upsertEveryStep *benchmarkStepUpsertEvery
	if *namespaceUpsertFrequency > 0 {
		upsertEveryStep = &benchmarkStepUpsertEvery{
			namespaces: br.namespaces,
			dataset:    br.dataset,
			frequency:  time.Second * time.Duration(*namespaceUpsertFrequency),
			batchSize:  *namespaceUpsertBatchSize,
			reports:    br.reporter,
		}
	}

	switch {
	case queryStep != nil && upsertEveryStep != nil:
		steps = append(
			steps,
			&concurrentBenchmarkStep{steps: []benchmarkStep{queryStep, upsertEveryStep}},
		)
	case queryStep != nil:
		steps = append(steps, queryStep)
	case upsertEveryStep != nil:
		steps = append(steps, upsertEveryStep)
	default:
		logger.Warn(
			"namespace-each-qps and namespace-each-upsert-frequency-s are both 0, no work to do",
		)
	}

	return steps
}

func (br *benchmarkRunner) anyExistingDocuments() bool {
	for _, ns := range br.namespaces {
		if ns.documents.Load() > 0 {
			return true
		}
	}
	return false
}

type benchmarkStep interface {
	desc() string
	run(ctx context.Context, logger *slog.Logger) error
}

type benchmarkStepDelete struct {
	namespaces []*namespace
}

func (d *benchmarkStepDelete) desc() string {
	return fmt.Sprintf("deleting existing data from %d namespaces", len(d.namespaces))
}

func (d *benchmarkStepDelete) run(ctx context.Context, logger *slog.Logger) error {
	logger.Info("deleting existing data from namespaces", slog.Int("count", len(d.namespaces)))
	var (
		eg  = new(errgroup.Group)
		bar = progressbar.Default(int64(len(d.namespaces)), "deleting documents")
	)
	eg.SetLimit(100)
	for _, ns := range d.namespaces {
		eg.Go(func() error {
			if err := ns.deleteAllDocuments(ctx); err != nil {
				return fmt.Errorf("deleting documents from namespace %s: %w", ns.handle.Name, err)
			}
			bar.Add(1)
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("deleting documents: %w", err)
	}
	return nil
}

type benchmarkStepSetup struct {
	namespaces []*namespace
	dataset    []turbopuffer.Document
	sizes      sizeHistogram
}

func (s *benchmarkStepSetup) desc() string {
	var builder strings.Builder
	builder.WriteString(
		fmt.Sprintf("setting up %d namespaces with documents:\n", len(s.namespaces)),
	)
	builder.WriteString(fmt.Sprintf("   - min size: %d\n", s.sizes.min()))
	builder.WriteString(fmt.Sprintf("   - max size: %d\n", s.sizes.max()))
	builder.WriteString(fmt.Sprintf("   - p10 size: %d\n", s.sizes.percentile(10)))
	builder.WriteString(fmt.Sprintf("   - p25 size: %d\n", s.sizes.percentile(25)))
	builder.WriteString(fmt.Sprintf("   - p50 size: %d\n", s.sizes.percentile(50)))
	builder.WriteString(fmt.Sprintf("   - p75 size: %d\n", s.sizes.percentile(75)))
	builder.WriteString(fmt.Sprintf("   - p90 size: %d\n", s.sizes.percentile(90)))
	builder.WriteString(fmt.Sprintf("   - p99 size: %d\n", s.sizes.percentile(99)))
	builder.WriteString(fmt.Sprintf("   - p999 size: %d\n", s.sizes.percentile(99.9)))
	builder.WriteString(
		fmt.Sprintf("   - total documents to upsert across all namespaces: %d\n", s.sizes.sum()),
	)
	return builder.String()
}

func (d *benchmarkStepSetup) run(ctx context.Context, logger *slog.Logger) error {
	if max := d.sizes.max(); max > len(d.dataset) {
		return fmt.Errorf("not enough documents in dataset, need at least %d", max)
	}
	var (
		eg    = new(errgroup.Group)
		total = d.sizes.sum()
		bar   = progressbar.Default(total, "upserting documents")
	)
	eg.SetLimit(runtime.NumCPU() - 1)
	for i, ns := range d.namespaces {
		size := d.sizes[i]
		eg.Go(func() error {
			if _, err := ns.upsertDocumentsBatched(ctx, d.dataset[:size]); err != nil {
				return fmt.Errorf("upserting documents to namespace %s: %w", ns.handle.Name, err)
			}
			bar.Add(size)
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("upserting documents: %w", err)
	}
	return nil
}

type benchmarkStepWaitForIndexing struct {
	namespaces          []*namespace
	exhaustiveThreshold int64
}

func (w *benchmarkStepWaitForIndexing) desc() string {
	var builder strings.Builder
	builder.WriteString("waiting for namespaces to be indexed after setup\n")
	var namespaces []string
	for _, ns := range w.namespaces {
		namespaces = append(namespaces, ns.handle.Name)
	}
	builder.WriteString(fmt.Sprintf("    - namespaces: %s\n", strings.Join(namespaces, ", ")))
	builder.WriteString(
		fmt.Sprintf(
			"    - wait until queries exhaustively search less than %d documents\n",
			w.exhaustiveThreshold,
		),
	)
	return builder.String()
}

func (w *benchmarkStepWaitForIndexing) run(ctx context.Context, logger *slog.Logger) error {
	var (
		wg = new(sync.WaitGroup)
		pg = progressbar.Default(int64(len(w.namespaces)), "waiting for indexing")
	)
	for _, ns := range w.namespaces {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer pg.Add(1)
			ns.waitForIndexing(ctx, logger, w.exhaustiveThreshold, time.Second*10)
		}()
	}
	wg.Wait()
	return nil
}

type benchmarkStepQuery struct {
	namespaces     []*namespace
	dataset        []turbopuffer.Document
	distributedQps float64
	warmupPeriod   time.Duration
	reports        *reporter
}

func (q *benchmarkStepQuery) desc() string {
	return fmt.Sprintf(
		"querying the set of %d namespaces at a combined rate of %f qps (%s warmup period)",
		len(q.namespaces),
		q.distributedQps,
		q.warmupPeriod.Round(time.Second),
	)
}

func (q *benchmarkStepQuery) run(ctx context.Context, logger *slog.Logger) error {
	if q.distributedQps == 0 {
		return nil
	}

	start := time.Now()
	for {
		currentQps := q.distributedQps
		if since := time.Since(start); since < q.warmupPeriod {
			currentQps = max(
				q.distributedQps/20,
				q.distributedQps*float64(since)/float64(q.warmupPeriod),
			)
		}
		interval := time.Duration(float64(time.Second) * (1 / currentQps))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
			targetNamespace := q.namespaces[rand.IntN(len(q.namespaces))]
			go func() {
				stats, err := targetNamespace.queryWithRandomDocumentVector(
					ctx,
					q.dataset,
				)
				if err != nil {
					logger.Warn(
						"failed to query namespace",
						slog.String("namespace", targetNamespace.handle.Name),
						slog.String("error", err.Error()),
					)
					return
				} else if q.reports != nil && stats != nil {
					q.reports.sendReport(ctx, report{query: stats})
				}
				logger.Debug(
					"queried namespace",
					slog.String("namespace", targetNamespace.handle.Name),
					slog.Duration("client latency", stats.clientLatency),
					slog.Duration("server latency", stats.serverLatency),
					slog.Int64("exhaustive count", stats.numExhaustive),
					slog.Int64("namespace size", stats.namespaceSize),
					slog.String(queryTemperatureAttrKey, string(stats.temperature)),
				)
			}()
		}
	}
}

type benchmarkStepUpsertEvery struct {
	namespaces []*namespace
	dataset    []turbopuffer.Document
	frequency  time.Duration
	batchSize  int
	reports    *reporter
}

func (u *benchmarkStepUpsertEvery) desc() string {
	return fmt.Sprintf(
		"upserting %d documents to each namespace (%d) every %s, until all namespaces reach max size (%d)",
		u.batchSize,
		len(u.namespaces),
		u.frequency,
		len(u.dataset),
	)
}

func (u *benchmarkStepUpsertEvery) run(ctx context.Context, logger *slog.Logger) error {
	wg := new(sync.WaitGroup)
	for _, ns := range u.namespaces {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ns.upsertBatchEvery(ctx, logger, u.dataset, u.frequency, u.batchSize, u.reports)
		}()
	}
	logger.Debug(
		"started upserting documents to namespaces",
		slog.Int("count", len(u.namespaces)),
		slog.Int("batch-size", u.batchSize),
		slog.Duration("frequency", u.frequency),
	)
	wg.Wait()
	return nil
}

type concurrentBenchmarkStep struct {
	steps []benchmarkStep
}

func (c *concurrentBenchmarkStep) desc() string {
	var builder strings.Builder
	builder.WriteString("concurrent steps:\n")
	for i, step := range c.steps {
		builder.WriteString(fmt.Sprintf("   %d. %s\n", i+1, step.desc()))
	}
	return builder.String()
}

func (c *concurrentBenchmarkStep) run(ctx context.Context, logger *slog.Logger) error {
	eg := new(errgroup.Group)
	for _, step := range c.steps {
		step := step
		eg.Go(func() error {
			return step.run(ctx, logger)
		})
	}
	return eg.Wait()
}

func loadDataset(
	ctx context.Context,
	maxNamespaceSize int,
) ([]turbopuffer.Document, error) {
	var (
		documents = make([]turbopuffer.Document, 0, maxNamespaceSize)
		bar       = progressbar.Default(int64(maxNamespaceSize), "loading dataset")
	)
	for doc, err := range documentsIter(ctx) {
		if err != nil {
			return nil, fmt.Errorf("loading dataset: %w", err)
		}
		documents = append(documents, doc)
		bar.Add(1)
		if len(documents) >= maxNamespaceSize {
			break
		}
	}
	return documents, nil
}

func loadNamespaces(
	ctx context.Context,
	client *turbopuffer.Client,
	prefix string,
	count int,
) ([]*namespace, error) {
	var (
		eg         = new(errgroup.Group)
		namespaces = make([]*namespace, count)
		bar        = progressbar.Default(int64(count), "syncing namespace state")
	)
	eg.SetLimit(100)
	for i := 0; i < count; i++ {
		var (
			i    = i
			name = fmt.Sprintf("%s_%d", prefix, i)
		)
		eg.Go(func() error {
			ns, err := loadNamespace(ctx, client, name)
			if err != nil {
				return fmt.Errorf("loading namespace %s: %w", name, err)
			}
			namespaces[i] = ns
			bar.Add(1)
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, fmt.Errorf("loading namespaces: %w", err)
	}
	return namespaces, nil
}
