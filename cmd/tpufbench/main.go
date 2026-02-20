package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/turbopuffer/tpuf-benchmark/pkg/bench"
	"github.com/turbopuffer/tpuf-benchmark/pkg/datasource"
	"github.com/turbopuffer/tpuf-benchmark/pkg/output"
	"github.com/turbopuffer/turbopuffer-go"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "tpufbench",
		Short: "turbopuffer benchmark CLI",
	}
	// Service config flags
	serviceCfg := bench.ServiceConfig{
		APIKey: os.Getenv("TURBOPUFFER_API_KEY"),
	}
	rootCmd.PersistentFlags().StringVar(&serviceCfg.Endpoint,
		"endpoint", "https://REGION.turbopuffer.com", "the turbopuffer endpoint to use")
	rootCmd.PersistentFlags().StringVar(&serviceCfg.HostHeader,
		"host-header", "", "an optional host header to include with turbopuffer requests")
	rootCmd.PersistentFlags().BoolVar(&serviceCfg.AllowTLSInsecure,
		"allow-tls-insecure", false, "allow insecure TLS connections to the turbopuffer API")

	addExecutionFlags := func(flags *pflag.FlagSet, cfg *bench.RuntimeConfig) {
		flags.StringVar(&cfg.NamespacePrefix, "namespace-prefix", hostname(),
			"a unique string to prefix namespace names with. defaults to your machine hostname")
		flags.IntVar(&cfg.NamespaceSetupConcurrency, "namespace-setup-concurrency", 4,
			"the number of concurrent goroutines to use for namespace setup (concurrency per namespace)")
		flags.IntVar(&cfg.NamespaceSetupConcurrencyMax, "namespace-setup-concurrency-max", 64,
			"maximum number of concurrent goroutines to use for namespace setup (total across all namespaces)")
		flags.StringVar(&cfg.IfNonempty, "if-nonempty", "abort",
			"behavior when namespaces already contain data: 'clear' to delete existing data, 'skip-upsert' to use as-is, 'abort' to stop with an error")
		flags.StringVar(&cfg.OutputDir, "output-dir", "",
			"directory to write benchmark results to. if empty, creates a new directory in results/")
		flags.BoolVar(&cfg.WarmCache, "warm-cache", false,
			"warm the cache of all namespaces before starting the benchmark")
		flags.BoolVar(&cfg.PurgeCache, "purge-cache", false,
			"purge the cache of all namespaces before starting the benchmark")
		flags.DurationVar(&cfg.Duration, "duration", 0,
			"override the benchmark duration (e.g. '5m', '1h'). if unset, uses the definition's duration")
	}

	// run command
	rootCmd.AddCommand(func() *cobra.Command {
		var cfg bench.RuntimeConfig
		runCmd := &cobra.Command{
			Use:   "run <definition-path>",
			Short: "Run an individual turbopuffer benchmark",
			RunE: func(cmd *cobra.Command, args []string) error {
				rctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
				defer cancel()
				logger := output.NewLogger(cmd.OutOrStdout())
				err := run(rctx, serviceCfg, cfg, logger, args[0])
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return err
			},
			Args: cobra.ExactArgs(1),
		}
		addExecutionFlags(runCmd.Flags(), &cfg)
		return runCmd
	}())

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(ctx context.Context, serviceCfg bench.ServiceConfig, cfg bench.RuntimeConfig, logger *output.Logger, definitionPath string) error {
	def, err := bench.ParseDefinition(definitionPath)
	if err != nil {
		return fmt.Errorf("parsing definition: %w", err)
	}
	if cfg.Duration > 0 {
		def.Duration = cfg.Duration
	}

	http.Handle("/metrics", promhttp.Handler())
	go func() {
		log.Println("debug server listening on :6060")
		if err := http.ListenAndServe(":6060", nil); err != nil {
			log.Printf("debug server error: %v", err)
		}
	}()

	if serviceCfg.Endpoint == "" {
		return errors.New("endpoint must be provided")
	} else if serviceCfg.APIKey == "" {
		return errors.New("TURBOPUFFER_API_KEY environment variable must be provided")
	}

	// Script should be run via a cloud VM
	likelyCloudVM, err := likelyRunningOnCloudVM(ctx)
	if err != nil {
		logger.Detailf("failed to determine if running on cloud VM: %v", err)
	} else if !likelyCloudVM {
		logger.Detailf("detected that this script isn't running on a cloud VM")
		logger.Detailf("for best results, this benchmark needs to be run within the same region as the turbopuffer deployment")
	}

	logger.NextStage(output.StageInit)

	// Ensure we're able to do requests against the API before downloading
	// datasets or initializing datasources.
	client := serviceCfg.NewClient()
	if err := runSanity(ctx, &client, cfg.NamespacePrefix, logger); err != nil {
		return fmt.Errorf("failed sanity check: %w", err)
	}

	datasourceCfg := datasource.Config{
		CacheDir:         datasetCacheDir(),
		ParseConcurrency: 2,
		Hooks: datasource.Hooks{
			OnDownload:       logger.OnDownload,
			OnLoadCachedFile: logger.OnLoadCachedFile,
		},
		// TODO(jackson): Add a CLI flag for the seed.
		Seed:             1,
		VectorDimensions: 1024,
	}
	if err := def.Init(ctx, datasourceCfg); err != nil {
		return fmt.Errorf("initializing datasource: %w", err)
	}

	if err = bench.Run(ctx, &client, def, cfg, logger); err != nil {
		return err
	}
	return nil
}

// runSanity performs a lightweight sanity check against the turbopuffer API
// to verify connectivity and basic ANN query functionality. It uses a small
// random vector dataset independent of the benchmark definition.
func runSanity(ctx context.Context, client *turbopuffer.Client, nsPrefix string, logger *output.Logger) error {
	logger.Detailf("running sanity check against API")
	ns := bench.NewNamespace(ctx, client, fmt.Sprintf("%s_sanity", nsPrefix))

	if err := ns.Clear(ctx); err != nil {
		return fmt.Errorf("deleting existing documents: %w", err)
	}

	// Build templates for a simple random-vector upsert and ANN query.
	ds := datasource.RandomDatasource(ctx, datasource.Config{
		Seed:             0,
		VectorDimensions: 1024,
	})
	funcs := ds.FuncMap(ctx)
	docTmpl, err := bench.NewTemplate(`{"id":{{id}},"vector":{{vector 1024}}}`, funcs)
	if err != nil {
		return fmt.Errorf("parsing sanity doc template: %w", err)
	}
	upsertTmpl, err := bench.NewTemplate(`{"upsert_rows":[{{.UpsertBatchPlaceholder}}], "distance_metric": "euclidean_squared"}`, funcs)
	if err != nil {
		return fmt.Errorf("parsing sanity upsert template: %w", err)
	}
	queryTmpl, err := bench.NewTemplate(`{"rank_by":["vector", "ANN", {{vector 1024}}],"top_k":5}`, funcs)
	if err != nil {
		return fmt.Errorf("parsing sanity query template: %w", err)
	}

	if _, _, err := ns.Upsert(ctx, 10, docTmpl, upsertTmpl); err != nil {
		return fmt.Errorf("upserting documents: %w", err)
	}

	serverTiming, clientDuration, err := ns.Query(ctx, 0, queryTmpl)
	if err != nil {
		return fmt.Errorf("querying namespace: %w", err)
	}

	// Best effort attempt to detect running against the wrong region by
	// detecting discrepancies between client and server query latency i.e. if
	// >10ms, probably running in different regions.
	if serverTiming != nil {
		serverMs := serverTiming.ServerTotalMs
		clientMs := clientDuration.Milliseconds()
		logger.Detailf("server timing: %dms, client timing: %dms", serverMs, clientMs)
		if serverMs+10 < clientMs {
			discrepancy := clientMs - serverMs
			logger.Detailf(
				"detected %d ms discrepancy between client and server query latency",
				discrepancy,
			)
			logger.Detailf("are you running this script in the same region as turbopuffer?")
		}
	}
	logger.Detailf("sanity check passed")
	return nil
}

func likelyRunningOnCloudVM(ctx context.Context) (bool, error) {
	// This endpoint is common with most cloud providers, aka should work on
	// GCP, AWS, Azure, etc. We use this to determine if we are running on a
	// cloud VM, and log a warning if we aren't.
	const metadataUrl = "169.254.169.254"

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

func hostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return hostname
}

func datasetCacheDir() string {
	dir := os.Getenv("DATASET_CACHE_DIR")
	if dir != "" {
		return dir
	}
	return os.TempDir()
}
