package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/spf13/cobra"
	"github.com/turbopuffer/tpuf-benchmark/pkg/bench"
	"github.com/turbopuffer/turbopuffer-go"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "tpufbench",
		Short: "turbopuffer benchmark CLI",
	}
	// Service config flags
	var serviceCfg bench.ServiceConfig
	rootCmd.PersistentFlags().StringVar(&serviceCfg.APIKey, "api-key", "", "the turbopuffer API key to use")
	rootCmd.PersistentFlags().StringVar(&serviceCfg.Endpoint, "endpoint", "", "the turbopuffer endpoint to use")
	rootCmd.PersistentFlags().StringVar(&serviceCfg.HostHeader, "host-header", "", "an optional host header to include with turbopuffer requests")
	rootCmd.PersistentFlags().BoolVar(&serviceCfg.AllowTLSInsecure, "allow-tls-insecure", false, "allow insecure TLS connections to the turbopuffer API")

	var cfg bench.RunConfig
	runCmd := &cobra.Command{
		Use:   "run",
		Short: "Run an individual turbopuffer benchmark",
		RunE: func(cmd *cobra.Command, args []string) error {
			rctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
			defer cancel()
			return run(rctx, cancel, serviceCfg, cfg)
		},
	}

	flags := runCmd.Flags()
	flags.StringVar(&cfg.UpsertTemplate, "upsert-template", "templates/upsert_default.json.tmpl", "template file for upsert requests")
	flags.StringVar(&cfg.DocumentTemplate, "document-template", "templates/document_default.json.tmpl", "template file for document generation")
	flags.StringVar(&cfg.QueryTemplate, "query-template", "templates/query_default.json.tmpl", "template file for query requests")
	flags.StringVar(&cfg.NamespacePrefix, "namespace-prefix", hostname(), "a unique string to prefix namespace names with. defaults to your machine hostname")
	flags.IntVar(&cfg.NamespaceCount, "namespace-count", 10, "the number of namespaces to operate on. namespaces are named <namespace-prefix>_<num>")
	flags.Int64Var(&cfg.NamespaceCombinedSize, "namespace-combined-size", 100_000, "combined number of documents distributed across all namespaces")
	flags.StringVar(&cfg.NamespaceSizeDistribution, "namespace-size-distribution", "uniform", "distribution of document counts across namespaces. options: 'uniform', 'lognormal'")
	flags.Float64Var(&cfg.LogNormalMu, "lognormal-mu", 0, "mu parameter for lognormal distribution of namespace sizes")
	flags.Float64Var(&cfg.LogNormalSigma, "lognormal-sigma", 0.95, "sigma parameter for lognormal distribution of namespace sizes")
	flags.IntVar(&cfg.NamespaceSetupConcurrency, "namespace-setup-concurrency", 4, "the number of concurrent goroutines to use for namespace setup (concurrency per namespace)")
	flags.IntVar(&cfg.NamespaceSetupConcurrencyMax, "namespace-setup-concurrency-max", 64, "maximum number of concurrent goroutines to use for namespace setup (total across all namespaces)")
	flags.IntVar(&cfg.NamespaceSetupBatchSize, "namespace-setup-batch-size", 250_000, "the number of documents to process in each batch during namespace setup")
	flags.BoolVar(&cfg.PromptToClear, "prompt-to-clear", true, "prompt the user to clear non-empty namespaces before starting the benchmark")
	flags.BoolVar(&cfg.WaitForIndexing, "wait-for-indexing", true, "wait for namespaces to be indexed after initial high-wps upserts, before starting benchmark")
	flags.BoolVar(&cfg.PurgeCache, "purge-cache", false, "purge the cache before starting the benchmark")
	flags.BoolVar(&cfg.WarmCache, "warm-cache", false, "warm the cache before starting the benchmark")
	flags.Float64Var(&cfg.QueriesPerSecond, "queries-per-sec", 3.0, "combined queries per second across all namespaces. see: `query-distribution`")
	flags.IntVar(&cfg.QueryConcurrency, "query-concurrency", 8, "the number of concurrent queries to run")
	flags.StringVar(&cfg.QueryDistribution, "query-distribution", "uniform", "distribution of queries across namespaces. options: 'uniform', 'pareto', 'round-robin'")
	flags.Float64Var(&cfg.QueryParetoAlpha, "query-pareto-alpha", 1.5, "alpha parameter for pareto distribution of queries")
	flags.Float64Var(&cfg.ActiveNamespacePct, "query-active-namespace-pct", 1.0, "the percentage of namespaces that will be queried. defaults to ~20%, conservative estimate from our production workloads")
	flags.IntVar(&cfg.UpsertsPerSecond, "upserts-per-sec", 5, "combined upserts per second across all namespaces. will respect `upsert-min-batch-size` and `upsert-max-batch-size`")
	flags.IntVar(&cfg.UpsertConcurrency, "upsert-concurrency", 8, "the number of concurrent upserts to run")
	flags.IntVar(&cfg.UpsertBatchSize, "upsert-batch-size", 1, "number of documents to upsert in a single request")
	flags.DurationVar(&cfg.Duration, "benchmark-duration", time.Minute*10, "duration of the benchmark. if 0, will run indefinitely")
	flags.IntVar(&cfg.QueryRetries, "query-retries", 0, "number of times to retry failed queries (default 0 = default tpuf client retries)")
	flags.StringVar(&cfg.OutputDir, "output-dir", defaultOutputDir(), "directory to write benchmark results to. if empty, won't write anything to disk")

	rootCmd.AddCommand(runCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(ctx context.Context, cancel context.CancelFunc, serviceCfg bench.ServiceConfig, cfg bench.RunConfig) error {
	if serviceCfg.Endpoint == "" {
		return errors.New("endpoint must be provided")
	} else if serviceCfg.APIKey == "" {
		return errors.New("api-key must be provided")
	}

	// Script should be run via a cloud VM
	likelyCloudVM, err := likelyRunningOnCloudVM(ctx)
	if err != nil {
		log.Printf("failed to determine if running on cloud VM: %v", err)
	} else if !likelyCloudVM {
		log.Printf("detected that this script isn't running on a cloud VM")
		log.Printf("for best results, this benchmark needs to be run within the same region as the turbopuffer deployment")
	}

	client := serviceCfg.NewClient()
	tmpls, err := bench.LoadTemplates(ctx, cfg)
	if err != nil {
		return fmt.Errorf("loading templates: %w", err)
	}

	// Make sure we're able to do requests against the API.
	if err := runSanity(ctx, &client, cfg.NamespacePrefix, tmpls); err != nil {
		return fmt.Errorf("failed sanity check: %w", err)
	}

	if err = bench.Run(ctx, cancel, &client, tmpls, cfg); err != nil {
		return err
	}
	return nil
}

// Runs a sanity check against the turbopuffer API to make sure
// that the API is up and running, and that we're able to do requests
// against it.
func runSanity(ctx context.Context, client *turbopuffer.Client, nsPrefix string, tmpls *bench.Templates) error {
	log.Print("running sanity check against API")
	ns := bench.NewNamespace(ctx, client, fmt.Sprintf("%s_sanity", nsPrefix), tmpls)

	if err := ns.Clear(ctx); err != nil {
		return fmt.Errorf("deleting existing documents: %w", err)
	}

	if _, _, err := ns.Upsert(ctx, 10); err != nil {
		return fmt.Errorf("upserting documents: %w", err)
	}

	serverTiming, clientDuration, err := ns.Query(ctx, 0)
	if err != nil {
		return fmt.Errorf("querying namespace: %w", err)
	}

	// Little helper to detect discrepancies between client and server query latency
	// i.e. if >10ms, probably running in different regions
	var (
		serverMs = serverTiming.ServerTotalMs
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

// We want to avoid collisions between multiple benchmark runs,
// i.e. we suffix the output directory with a timestamp
func defaultOutputDir() string {
	return fmt.Sprintf("benchmark-results-%d", time.Now().Unix())
}
