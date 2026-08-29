package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/turbopuffer/tpuf-benchmark/pkg/bench"
	"github.com/turbopuffer/tpuf-benchmark/pkg/datasource"
	"github.com/turbopuffer/tpuf-benchmark/pkg/output"
	"github.com/turbopuffer/turbopuffer-go"
)

// Environment variables that can be used as fallbacks for run flags.
const (
	envEndpoint                     = "TPUFBENCH_ENDPOINT"
	envHostHeader                   = "TPUFBENCH_HOST_HEADER"
	envAllowTLSInsecure             = "TPUFBENCH_ALLOW_TLS_INSECURE"
	envNamespacePrefix              = "TPUFBENCH_NAMESPACE_PREFIX"
	envNamespaceSetupConcurrency    = "TPUFBENCH_NAMESPACE_SETUP_CONCURRENCY"
	envNamespaceSetupConcurrencyMax = "TPUFBENCH_NAMESPACE_SETUP_CONCURRENCY_MAX"
	envIfNonempty                   = "TPUFBENCH_IF_NONEMPTY"
	envOutputDir                    = "TPUFBENCH_OUTPUT_DIR"
	envWarmCache                    = "TPUFBENCH_WARM_CACHE"
	envPurgeCache                   = "TPUFBENCH_PURGE_CACHE"
	envDuration                     = "TPUFBENCH_DURATION"
)

// envHelp appends the environment variable name to a flag description.
func envHelp(description, envName string) string {
	return fmt.Sprintf("%s (env: %s)", description, envName)
}

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
		"endpoint", "https://REGION.turbopuffer.com", envHelp("the turbopuffer endpoint to use", envEndpoint))
	rootCmd.PersistentFlags().StringVar(&serviceCfg.HostHeader,
		"host-header", "", envHelp("an optional host header to include with turbopuffer requests", envHostHeader))
	rootCmd.PersistentFlags().BoolVar(&serviceCfg.AllowTLSInsecure,
		"allow-tls-insecure", false, envHelp("allow insecure TLS connections to the turbopuffer API", envAllowTLSInsecure))

	addExecutionFlags := func(flags *pflag.FlagSet, cfg *bench.RuntimeConfig) {
		flags.StringVar(&cfg.NamespacePrefix, "namespace-prefix", "",
			envHelp("a unique string to prefix namespace names with. If unset, defaults to your machine hostname and the definition name", envNamespacePrefix))
		flags.IntVar(&cfg.NamespaceSetupConcurrency, "namespace-setup-concurrency", 4,
			envHelp("the maximum number of concurrent requests per namespace when upserting documents to setup a namespace", envNamespaceSetupConcurrency))
		flags.IntVar(&cfg.NamespaceSetupConcurrencyMax, "namespace-setup-concurrency-max", 64,
			envHelp("maximum number of concurrent requests when upserting documetnts to setup namespaces (across all namespaces)", envNamespaceSetupConcurrencyMax))
		flags.StringVar(&cfg.IfNonempty, "if-nonempty", "abort",
			envHelp("behavior when namespaces already contain data: 'clear' to delete existing data, 'skip-upsert' to use as-is, 'abort' to stop with an error", envIfNonempty))
		flags.StringVar(&cfg.OutputDir, "output-dir", "",
			envHelp("directory to write benchmark results to. if empty, creates a new directory in results/", envOutputDir))
		flags.BoolVar(&cfg.WarmCache, "warm-cache", false,
			envHelp("warm the cache of all namespaces before starting the benchmark", envWarmCache))
		flags.BoolVar(&cfg.PurgeCache, "purge-cache", false,
			envHelp("purge the cache of all namespaces before starting the benchmark", envPurgeCache))
		flags.DurationVar(&cfg.Duration, "duration", 0,
			envHelp("override the benchmark duration (e.g. '5m', '1h'). if unset, uses the definition's duration", envDuration))
	}

	// run command
	rootCmd.AddCommand(func() *cobra.Command {
		var cfg bench.RuntimeConfig
		runCmd := &cobra.Command{
			Use:   "run <definition-path>",
			Short: "Run an individual turbopuffer benchmark",
			RunE: func(cmd *cobra.Command, args []string) error {
				if err := applyEnvFallbacks(runEnvFallbacks, cmd.Flags(), cmd.InheritedFlags()); err != nil {
					return err
				}

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

	// list command
	rootCmd.AddCommand(func() *cobra.Command {
		var nightlyOnly bool
		listCmd := &cobra.Command{
			Use:   "list <dir>...",
			Short: "List benchmark definition files found under the given directories",
			RunE: func(cmd *cobra.Command, args []string) error {
				return list(cmd.OutOrStdout(), args, nightlyOnly)
			},
			Args: cobra.MinimumNArgs(1),
		}
		listCmd.Flags().BoolVar(&nightlyOnly, "nightly", false,
			"only list benchmarks marked with 'nightly = true'")
		return listCmd
	}())

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

// envFallback maps a flag to the environment variable that can configure it.
type envFallback struct {
	FlagName string
	EnvName  string
}

// runEnvFallbacks lists the environment fallbacks supported by tpufbench run.
var runEnvFallbacks = []envFallback{
	{FlagName: "endpoint", EnvName: envEndpoint},
	{FlagName: "host-header", EnvName: envHostHeader},
	{FlagName: "allow-tls-insecure", EnvName: envAllowTLSInsecure},
	{FlagName: "namespace-prefix", EnvName: envNamespacePrefix},
	{FlagName: "namespace-setup-concurrency", EnvName: envNamespaceSetupConcurrency},
	{FlagName: "namespace-setup-concurrency-max", EnvName: envNamespaceSetupConcurrencyMax},
	{FlagName: "if-nonempty", EnvName: envIfNonempty},
	{FlagName: "output-dir", EnvName: envOutputDir},
	{FlagName: "warm-cache", EnvName: envWarmCache},
	{FlagName: "purge-cache", EnvName: envPurgeCache},
	{FlagName: "duration", EnvName: envDuration},
}

// applyEnvFallbacks applies env values for flags that were not set explicitly.
func applyEnvFallbacks(fallbacks []envFallback, flagSets ...*pflag.FlagSet) error {
	for _, fallback := range fallbacks {
		flag := lookupFlag(fallback.FlagName, flagSets...)
		if flag == nil {
			return fmt.Errorf("flag %q not found for environment fallback %s", fallback.FlagName, fallback.EnvName)
		}
		if flag.Changed {
			continue
		}
		value, ok := os.LookupEnv(fallback.EnvName)
		if !ok || value == "" {
			continue
		}
		if err := flag.Value.Set(value); err != nil {
			return fmt.Errorf("invalid %s=%q for --%s: %w", fallback.EnvName, value, fallback.FlagName, err)
		}
	}
	return nil
}

// lookupFlag finds a flag by name across multiple flag sets.
func lookupFlag(name string, flagSets ...*pflag.FlagSet) *pflag.Flag {
	for _, flags := range flagSets {
		if flags == nil {
			continue
		}
		if flag := flags.Lookup(name); flag != nil {
			return flag
		}
	}
	return nil
}

func run(ctx context.Context, serviceCfg bench.ServiceConfig, cfg bench.RuntimeConfig, logger *output.Logger, definitionPath string) error {
	def, err := bench.ParseDefinition(definitionPath)
	if err != nil {
		return fmt.Errorf("parsing definition: %w", err)
	}
	if cfg.Duration > 0 {
		def.Duration = cfg.Duration
	}
	if cfg.NamespacePrefix == "" {
		cfg.NamespacePrefix = fmt.Sprintf("%s_%s", hostname(), def.Name)
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
	cacheDir := datasetCacheDir()
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return fmt.Errorf("creating cache directory: %w", err)
	}

	// Ensure we're able to do requests against the API before downloading
	// datasets or initializing datasources.
	client := serviceCfg.NewClient()
	if err := runSanity(ctx, &client, cfg.NamespacePrefix+"_sanity", logger); err != nil {
		return fmt.Errorf("failed sanity check: %w", err)
	}
	datasourceCfg := datasource.Config{
		CacheDir:         cacheDir,
		ParseConcurrency: 2,
		Hooks: datasource.Hooks{
			OnDownload:       logger.OnDownload,
			OnLoadCachedFile: logger.OnLoadCachedFile,
		},
		// TODO(jackson): Add a CLI flag for the seed.
		Seed: 1,
		// TODO(jackson): Allow configuring the number of vector dimensions per
		// benchmark. Currently all the supported datasources have 1024
		// dimensions.
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

// list walks the given directories for benchmark definition files (*.toml),
// parses each one, and writes the path of every match to w (one per line).
// When nightlyOnly is set, only definitions with 'nightly = true' are listed.
// Paths are sorted for deterministic output.
func list(w io.Writer, dirs []string, nightlyOnly bool) error {
	var paths []string
	for _, dir := range dirs {
		err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".toml") {
				return nil
			}
			def, err := bench.ParseDefinition(path)
			if err != nil {
				return fmt.Errorf("parsing %s: %w", path, err)
			}
			if nightlyOnly && !def.Nightly {
				return nil
			}
			paths = append(paths, path)
			return nil
		})
		if err != nil {
			return err
		}
	}
	sort.Strings(paths)
	for _, p := range paths {
		if _, err := fmt.Fprintln(w, p); err != nil {
			return err
		}
	}
	return nil
}

// runSanity performs a lightweight sanity check against the turbopuffer API
// to verify connectivity and basic ANN query functionality. It uses a small
// random vector dataset independent of the benchmark definition.
//
// TODO(jackson): runSanity uses a random workload right now. We could consider
// using the configured benchmark but just a few documents and queries in order
// to validate the benchmark definition.
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
	queryTmpl, err := bench.NewTemplate(`{"rank_by":["vector", "ANN", {{vector 1024}}],"top_k":8}`, funcs)
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
		panic(err)
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
