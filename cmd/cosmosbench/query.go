package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/spf13/cobra"
	"github.com/turbopuffer/tpuf-benchmark/pkg/datasource"
	"golang.org/x/sync/errgroup"
)

type queryConfig struct {
	database    string
	mode        string
	count       int
	topK        int
	concurrency int
	recall      bool
	cacheDir    string
}

type queryResult struct {
	latency time.Duration
	ru      float64
	recall  float64
}

func newQueryCommand() *cobra.Command {
	cfg := queryConfig{}
	cmd := &cobra.Command{
		Use:   "query",
		Short: "Run ANN or full-text queries",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := commandContext()
			defer cancel()
			return runQuery(ctx, cmd, cfg)
		},
	}
	cmd.Flags().StringVar(&cfg.database, "database", "tpufbench", "database name")
	cmd.Flags().StringVar(&cfg.mode, "mode", "ann", "query mode: ann or fts")
	cmd.Flags().IntVar(&cfg.count, "count", 100, "number of queries")
	cmd.Flags().IntVar(&cfg.topK, "top-k", 10, "number of results per query")
	cmd.Flags().IntVar(&cfg.concurrency, "concurrency", 8, "concurrent queries")
	cmd.Flags().BoolVar(&cfg.recall, "recall", false, "compare ANN results with brute force")
	cmd.Flags().StringVar(&cfg.cacheDir, "cache-dir", datasetCacheDir(), "datasource cache directory")
	return cmd
}

func runQuery(ctx context.Context, cmd *cobra.Command, cfg queryConfig) error {
	if cfg.mode != "ann" && cfg.mode != "fts" {
		return fmt.Errorf("mode must be ann or fts, got %q", cfg.mode)
	}
	if cfg.recall && cfg.mode != "ann" {
		return errors.New("recall is available only in ann mode")
	}
	if cfg.count <= 0 || cfg.topK <= 0 || cfg.concurrency <= 0 {
		return errors.New("count, top-k, and concurrency must be positive")
	}
	client, err := cosmosClient()
	if err != nil {
		return err
	}
	database, err := client.NewDatabase(cfg.database)
	if err != nil {
		return fmt.Errorf("opening database %s: %w", cfg.database, err)
	}
	containerName := "vectors"
	if cfg.mode == "fts" {
		containerName = "text"
	}
	container, err := database.NewContainer(containerName)
	if err != nil {
		return fmt.Errorf("opening container %s: %w", containerName, err)
	}
	queries, err := datasource.CohereMSMarcoQueryRows(ctx, datasource.Config{CacheDir: cfg.cacheDir, ParseConcurrency: 1})
	if err != nil {
		return fmt.Errorf("loading MSMarco queries: %w", err)
	}
	if cfg.count > len(queries) {
		return fmt.Errorf("requested %d queries, dataset contains %d", cfg.count, len(queries))
	}
	queries = queries[:cfg.count]

	results := make([]queryResult, len(queries))
	work := make(chan int)
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		defer close(work)
		for i := range queries {
			select {
			case work <- i:
			case <-groupCtx.Done():
				return groupCtx.Err()
			}
		}
		return nil
	})
	for range min(cfg.concurrency, len(queries)) {
		group.Go(func() error {
			for i := range work {
				query := queries[i]
				start := time.Now()
				ids, charge, err := executeBenchmarkQuery(groupCtx, container, cfg.mode, cfg.topK, query, false)
				latency := time.Since(start)
				if err != nil {
					return fmt.Errorf("query %d: %w", i, err)
				}
				result := queryResult{latency: latency, ru: charge}
				if cfg.recall {
					exactIDs, _, err := executeBenchmarkQuery(groupCtx, container, cfg.mode, cfg.topK, query, true)
					if err != nil {
						return fmt.Errorf("brute-force query %d: %w", i, err)
					}
					result.recall = recallAtK(ids, exactIDs, cfg.topK)
				}
				results[i] = result
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return err
	}
	printQuerySummary(cmd, cfg, results)
	return nil
}

func executeBenchmarkQuery(ctx context.Context, container *azcosmos.ContainerClient, mode string, topK int, row datasource.MSMarcoQueryRow, bruteForce bool) ([]string, float64, error) {
	var query string
	options := &azcosmos.QueryOptions{}
	if mode == "ann" {
		force := ""
		if bruteForce {
			force = ", true"
		}
		query = fmt.Sprintf("SELECT TOP %d c.id FROM c ORDER BY VectorDistance(c.vector, @qv%s)", topK, force)
		options.QueryParameters = []azcosmos.QueryParameter{{Name: "@qv", Value: row.Vector}}
	} else {
		literal, err := json.Marshal(row.Text)
		if err != nil {
			return nil, 0, fmt.Errorf("escaping full-text query: %w", err)
		}
		query = fmt.Sprintf("SELECT TOP %d c.id FROM c ORDER BY RANK FullTextScore(c.text, %s)", topK, literal)
	}
	crossPartition := true
	options.EnableCrossPartitionQuery = &crossPartition
	pager := container.NewQueryItemsPager(query, azcosmos.NewPartitionKey(), options)
	var ids []string
	var charge float64
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, 0, err
		}
		charge += float64(page.RequestCharge)
		for _, item := range page.Items {
			var result struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal(item, &result); err != nil {
				return nil, 0, fmt.Errorf("decoding query result: %w", err)
			}
			ids = append(ids, result.ID)
		}
	}
	return ids, charge, nil
}

func recallAtK(actual, expected []string, k int) float64 {
	exact := make(map[string]struct{}, min(k, len(expected)))
	for _, id := range expected[:min(k, len(expected))] {
		exact[id] = struct{}{}
	}
	matches := 0
	for _, id := range actual[:min(k, len(actual))] {
		if _, ok := exact[id]; ok {
			matches++
		}
	}
	return float64(matches) / float64(k)
}

func printQuerySummary(cmd *cobra.Command, cfg queryConfig, results []queryResult) {
	latencies := make([]float64, len(results))
	var totalRU float64
	var totalRecall float64
	for i, result := range results {
		latencies[i] = float64(result.latency) / float64(time.Millisecond)
		totalRU += result.ru
		totalRecall += result.recall
	}
	sort.Float64s(latencies)
	fmt.Fprintf(cmd.OutOrStdout(), "queries: %d\n", len(results))
	fmt.Fprintf(cmd.OutOrStdout(), "latency ms: p50 %.2f, p95 %.2f, p99 %.2f\n", percentile(latencies, 0.50), percentile(latencies, 0.95), percentile(latencies, 0.99))
	fmt.Fprintf(cmd.OutOrStdout(), "mean RU/query: %.2f\n", totalRU/float64(len(results)))
	if cfg.recall {
		fmt.Fprintf(cmd.OutOrStdout(), "mean recall@%d: %.4f\n", cfg.topK, totalRecall/float64(len(results)))
	}
}

func percentile(sorted []float64, p float64) float64 {
	index := max(0, int(math.Ceil(p*float64(len(sorted))))-1)
	return sorted[index]
}
