package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
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

	if *namespaceCount == 0 {
		logger.Error("namespace-count must be greater than 0")
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

	runner := &benchmarkRunner{
		namespaces: namespaces,
		dataset:    dataset,
	}

	steps, err := runner.plan(logger)
	if err != nil {
		return fmt.Errorf("planning benchmark steps: %w", err)
	} else if len(steps) == 0 {
		logger.Warn("no benchmark steps to run, maybe check your configuration?")
		return nil
	}

	for _, step := range steps {
		fmt.Println("")
		if err := step.before(); err != nil {
			return fmt.Errorf("step before: %w", err)
		}
		fmt.Println("")
		if err := step.run(ctx, logger); err != nil {
			return fmt.Errorf("step run: %w", err)
		}
		fmt.Println("")
		if err := step.after(); err != nil {
			return fmt.Errorf("step after: %w", err)
		}
		fmt.Println("")
	}

	return nil
}

type benchmarkStep interface {
	before() error
	run(context.Context, *slog.Logger) error
	after() error
}

type benchmarkRunner struct {
	namespaces []*namespace
	dataset    []turbopuffer.Document
}

func (br *benchmarkRunner) plan(logger *slog.Logger) ([]benchmarkStep, error) {
	var steps []benchmarkStep

	if *overrideExisting {
		steps = append(steps, &benchmarkStepDelete{namespaces: br.namespaces})
	}

	if !br.anyExistingDocuments() || *overrideExisting {
		var sizes sizeHistogram
		switch *namespaceInitialSize {
		case "min":
			sizes = generateSizesUniform(len(br.namespaces), *namespaceSizeMin)
		case "max":
			sizes = generateSizesUniform(len(br.namespaces), *namespaceSizeMax)
		case "lognormal":
			sizes = generateSizesLognormal(
				len(br.namespaces),
				*namespaceSizeMin,
				*namespaceSizeMax,
				*setupLognormalMu,
				*setupLognormalSigma,
			)
		default:
			return nil, fmt.Errorf("unknown namespace initial size: %s", *namespaceInitialSize)
		}

		steps = append(steps, &benchmarkStepUpsert{
			namespaces: br.namespaces,
			dataset:    br.dataset,
			sizes:      sizes,
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

		var queryDist queryDistribution
		switch *namespaceQueryDistribution {
		case "uniform":
			queryDist = &uniformDistribution{}
		case "pareto":
			queryDist = &paretoDistribution{
				alpha: *queryParetoAlpha,
				src:   rand.NewPCG(1, 2),
			}
		default:
			return nil, fmt.Errorf("unknown query distribution: %s", *namespaceQueryDistribution)
		}

		if *activeNamespacePct == 0 || *activeNamespacePct > 1 {
			return nil, fmt.Errorf("invalid active namespace percentage: %f", *activeNamespacePct)
		}

		queryStep = &benchmarkStepQuery{
			namespaces:     br.namespaces,
			dataset:        br.dataset,
			distributedQps: *namespaceDistributedQps,
			warmupPeriod:   warmupPeriod,
			queryDist:      queryDist,
			activePct:      *activeNamespacePct,
			reportInterval: *reportInterval,
			queryHeadstart: *queryHeadstart,
		}
	}

	var upsertAtRateStep *benchmarkStepUpsertAtRate
	if *namespaceUpsertFrequency > 0 {
		upsertAtRateStep = &benchmarkStepUpsertAtRate{
			namespaces:     br.namespaces,
			dataset:        br.dataset,
			frequency:      time.Second * time.Duration(*namespaceUpsertFrequency),
			batchSize:      *namespaceUpsertBatchSize,
			reportInterval: *reportInterval,
		}
	}

	var concurrentSteps []benchmarkStep
	if queryStep != nil {
		concurrentSteps = append(concurrentSteps, queryStep)
	}
	if upsertAtRateStep != nil {
		concurrentSteps = append(concurrentSteps, upsertAtRateStep)
	}

	if len(concurrentSteps) > 0 {
		var step benchmarkStep = &concurrentBenchmarkStep{steps: concurrentSteps}
		if *steadyStateDuration > 0 {
			step = &stopAfterBenchmarkStep{inner: step, stopAfter: *steadyStateDuration}
		}
		steps = append(steps, step)
	}

	return steps, nil
}

func (br *benchmarkRunner) anyExistingDocuments() bool {
	for _, ns := range br.namespaces {
		if ns.documents.Load() > 0 {
			return true
		}
	}
	return false
}

type concurrentBenchmarkStep struct {
	steps []benchmarkStep
}

func (c *concurrentBenchmarkStep) before() error {
	for _, step := range c.steps {
		if err := step.before(); err != nil {
			return err
		}
	}
	return nil
}

func (c *concurrentBenchmarkStep) after() error {
	for _, step := range c.steps {
		if err := step.after(); err != nil {
			return err
		}
	}
	return nil
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

type stopAfterBenchmarkStep struct {
	inner     benchmarkStep
	stopAfter time.Duration
}

func (s *stopAfterBenchmarkStep) before() error {
	fmt.Printf("Running for %s:\n", s.stopAfter)
	if err := s.inner.before(); err != nil {
		return err
	}
	return nil
}

func (s *stopAfterBenchmarkStep) after() error {
	fmt.Printf("Stopping after %s\n", s.stopAfter)
	if err := s.inner.after(); err != nil {
		return err
	}
	return nil
}

func (s *stopAfterBenchmarkStep) run(ctx context.Context, logger *slog.Logger) error {
	innerCtx, cancel := context.WithTimeout(ctx, s.stopAfter)
	defer cancel()
	return s.inner.run(innerCtx, logger)
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
