package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/spf13/cobra"
	"github.com/turbopuffer/tpuf-benchmark/pkg/datasource"
	"golang.org/x/sync/errgroup"
)

type ingestConfig struct {
	database    string
	rows        int
	startRow    int
	concurrency int
	containers  string
	cacheDir    string
}

type ingestJob struct {
	container string
	client    *azcosmos.ContainerClient
	id        string
	body      []byte
}

type ingestStats struct {
	mu           sync.Mutex
	docs         map[string]int64
	ru           map[string]float64
	conflictRU   map[string]float64
	conflicts    map[string]int64
	throttles    map[string]int64
	logicalBytes int64
}

type createItemResult struct {
	requestCharge float32
	conflict      bool
	throttles     int64
}

func newIngestCommand() *cobra.Command {
	cfg := ingestConfig{}
	cmd := &cobra.Command{
		Use:   "ingest",
		Short: "Ingest MSMarco passages",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := commandContext()
			defer cancel()
			return runIngest(ctx, cmd, cfg)
		},
	}
	cmd.Flags().StringVar(&cfg.database, "database", "tpufbench", "database name")
	cmd.Flags().IntVar(&cfg.rows, "rows", 1000000, "number of rows to ingest")
	cmd.Flags().IntVar(&cfg.startRow, "start-row", 0, "skip dataset rows below this index without writing (cheap resume)")
	cmd.Flags().IntVar(&cfg.concurrency, "concurrency", 64, "total concurrent item writes")
	cmd.Flags().StringVar(&cfg.containers, "containers", "vectors,text", "comma-separated containers to ingest")
	cmd.Flags().StringVar(&cfg.cacheDir, "cache-dir", datasetCacheDir(), "datasource cache directory")
	return cmd
}

func runIngest(ctx context.Context, cmd *cobra.Command, cfg ingestConfig) error {
	if cfg.rows <= 0 {
		return errors.New("rows must be positive")
	}
	if cfg.concurrency <= 0 {
		return errors.New("concurrency must be positive")
	}
	names, err := selectedContainers(cfg.containers)
	if err != nil {
		return err
	}
	client, err := cosmosClient()
	if err != nil {
		return err
	}
	database, err := client.NewDatabase(cfg.database)
	if err != nil {
		return fmt.Errorf("opening database %s: %w", cfg.database, err)
	}
	clients := make(map[string]*azcosmos.ContainerClient, len(names))
	for _, name := range names {
		clients[name], err = database.NewContainer(name)
		if err != nil {
			return fmt.Errorf("opening container %s: %w", name, err)
		}
	}

	stats := &ingestStats{
		docs:       make(map[string]int64),
		ru:         make(map[string]float64),
		conflictRU: make(map[string]float64),
		conflicts:  make(map[string]int64),
		throttles:  make(map[string]int64),
	}
	jobs := make(chan ingestJob, cfg.concurrency)
	group, groupCtx := errgroup.WithContext(ctx)
	for range cfg.concurrency {
		group.Go(func() error {
			for job := range jobs {
				result, err := createItemWithRetry(groupCtx, job.client, job.id, job.body)
				stats.addThrottles(job.container, result.throttles)
				if err != nil {
					return fmt.Errorf("creating item %s in %s: %w", job.id, job.container, err)
				}
				stats.addWrite(job.container, result)
			}
			return nil
		})
	}
	group.Go(func() error {
		defer close(jobs)
		rowsRead := 0
		seq := datasource.CohereMSMarcoRows(groupCtx, datasource.Config{CacheDir: cfg.cacheDir, ParseConcurrency: 2})
		for row, rowErr := range seq {
			if rowErr != nil {
				return fmt.Errorf("reading MSMarco rows: %w", rowErr)
			}
			if rowsRead < cfg.startRow {
				rowsRead++
				continue
			}
			id := strconv.Itoa(rowsRead)
			bodies, err := marshalIngestBodies(id, row, names)
			if err != nil {
				return fmt.Errorf("marshalling row %s: %w", id, err)
			}
			logical := 0
			for _, name := range names {
				logical += len(row.Text)
				if name == "vectors" {
					logical += 2 * len(row.Vector)
				}
			}
			stats.addLogicalBytes(int64(logical))
			for _, name := range names {
				select {
				case jobs <- ingestJob{container: name, client: clients[name], id: id, body: bodies[name]}:
				case <-groupCtx.Done():
					return groupCtx.Err()
				}
			}
			rowsRead++
			if rowsRead == cfg.rows {
				return nil
			}
		}
		return fmt.Errorf("dataset exhausted after %d rows", rowsRead)
	})

	start := time.Now()
	reporterDone := make(chan struct{})
	reporterStopped := make(chan struct{})
	go reportIngestProgress(cmd, start, names, stats, reporterDone, reporterStopped)
	err = group.Wait()
	close(reporterDone)
	<-reporterStopped
	if err != nil {
		if errors.Is(err, context.Canceled) {
			fmt.Fprintln(cmd.ErrOrStderr(), "ingest canceled")
		}
		return err
	}
	printIngestSummary(cmd, time.Since(start), names, stats)
	return nil
}

func selectedContainers(value string) ([]string, error) {
	seen := make(map[string]bool)
	var names []string
	for _, raw := range strings.Split(value, ",") {
		name := strings.TrimSpace(raw)
		if name != "vectors" && name != "text" {
			return nil, fmt.Errorf("containers must contain only vectors and text, got %q", name)
		}
		if !seen[name] {
			seen[name] = true
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return nil, errors.New("at least one container must be selected")
	}
	return names, nil
}

func marshalIngestBodies(id string, row datasource.MSMarcoRow, names []string) (map[string][]byte, error) {
	bodies := make(map[string][]byte, len(names))
	for _, name := range names {
		var value any
		if name == "vectors" {
			value = struct {
				ID     string    `json:"id"`
				PK     string    `json:"pk"`
				Vector []float32 `json:"vector"`
				Text   string    `json:"text"`
			}{id, id, row.Vector, row.Text}
		} else {
			value = struct {
				ID   string `json:"id"`
				PK   string `json:"pk"`
				Text string `json:"text"`
			}{id, id, row.Text}
		}
		body, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		bodies[name] = body
	}
	return bodies, nil
}

func createItemWithRetry(ctx context.Context, client *azcosmos.ContainerClient, id string, body []byte) (createItemResult, error) {
	const attempts = 10
	var result createItemResult
	delay := 25 * time.Millisecond
	failures := 0
	for {
		response, err := client.CreateItem(ctx, azcosmos.NewPartitionKeyString(id), body, &azcosmos.ItemOptions{EnableContentResponseOnWrite: false})
		if err == nil {
			result.requestCharge = response.RequestCharge
			return result, nil
		}
		var responseErr *azcore.ResponseError
		wait := time.Duration(0)
		if errors.As(err, &responseErr) && responseErr.StatusCode == http.StatusConflict {
			result.conflict = true
			if responseErr.RawResponse != nil {
				if v, perr := strconv.ParseFloat(responseErr.RawResponse.Header.Get("x-ms-request-charge"), 64); perr == nil {
					result.requestCharge = float32(v)
				}
			}
			return result, nil
		}
		if errors.As(err, &responseErr) && responseErr.StatusCode == http.StatusTooManyRequests {
			// Throttling is pacing, not failure: wait as long as the server
			// says, plus jitter so workers don't convoy into 429 waves.
			result.throttles++
			wait = 50 * time.Millisecond
			if responseErr.RawResponse != nil {
				if ms := responseErr.RawResponse.Header.Get("x-ms-retry-after-ms"); ms != "" {
					if v, perr := strconv.ParseFloat(ms, 64); perr == nil && v > 0 {
						wait = time.Duration(v * float64(time.Millisecond))
					}
				}
			}
			wait += rand.N(wait/2 + time.Millisecond)
		} else {
			failures++
			fmt.Fprintf(os.Stderr, "!! %s item %s failed (attempt %d/%d): %v\n",
				time.Now().UTC().Format(time.RFC3339), id, failures, attempts, err)
			if !retryableCosmosError(err) || failures == attempts {
				return result, err
			}
			wait = delay + rand.N(delay)
			delay = min(delay*2, time.Second)
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return result, ctx.Err()
		}
	}
}

func retryableCosmosError(err error) bool {
	var responseErr *azcore.ResponseError
	if errors.As(err, &responseErr) {
		return responseErr.StatusCode == http.StatusTooManyRequests || responseErr.StatusCode >= 500
	}
	// Transport-level failures (timeouts, resets) carry no HTTP status and
	// are always worth retrying on a long bulk load.
	return true
}

func (s *ingestStats) addWrite(container string, result createItemResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.docs[container]++
	if result.conflict {
		s.conflicts[container]++
		s.conflictRU[container] += float64(result.requestCharge)
	} else {
		s.ru[container] += float64(result.requestCharge)
	}
}

func (s *ingestStats) addThrottles(container string, throttles int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.throttles[container] += throttles
}

func (s *ingestStats) addLogicalBytes(bytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logicalBytes += bytes
}

func (s *ingestStats) snapshot(names []string) (int64, map[string]int64, map[string]float64, map[string]int64, map[string]int64, int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	docs := make(map[string]int64, len(names))
	ru := make(map[string]float64, len(names))
	conflicts := make(map[string]int64, len(names))
	throttles := make(map[string]int64, len(names))
	rows := int64(-1)
	for _, name := range names {
		docs[name] = s.docs[name]
		ru[name] = s.ru[name]
		conflicts[name] = s.conflicts[name]
		throttles[name] = s.throttles[name]
		if rows < 0 || docs[name] < rows {
			rows = docs[name]
		}
	}
	return rows, docs, ru, conflicts, throttles, s.logicalBytes
}

func reportIngestProgress(cmd *cobra.Command, start time.Time, names []string, stats *ingestStats, done <-chan struct{}, stopped chan<- struct{}) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	defer close(stopped)
	for {
		select {
		case <-ticker.C:
			rows, docs, ru, conflicts, throttles, _ := stats.snapshot(names)
			fmt.Fprintf(cmd.OutOrStdout(), "rows written: %d (%.1f rows/s)", rows, float64(rows)/time.Since(start).Seconds())
			for _, name := range names {
				mean := 0.0
				if docs[name] > 0 {
					mean = ru[name] / float64(docs[name])
				}
				fmt.Fprintf(cmd.OutOrStdout(), "; %s: %.2f mean RU/doc, %.2f cumulative RU", name, mean, ru[name])
				if conflicts[name] > 0 {
					fmt.Fprintf(cmd.OutOrStdout(), ", %d conflicts", conflicts[name])
				}
				if throttles[name] > 0 {
					fmt.Fprintf(cmd.OutOrStdout(), ", %d throttles", throttles[name])
				}
			}
			fmt.Fprintln(cmd.OutOrStdout())
		case <-done:
			return
		}
	}
}

func printIngestSummary(cmd *cobra.Command, elapsed time.Duration, names []string, stats *ingestStats) {
	rows, docs, ru, conflicts, throttles, logicalBytes := stats.snapshot(names)
	fmt.Fprintf(cmd.OutOrStdout(), "ingest complete: %d rows in %s (%.1f rows/s)\n", rows, elapsed.Round(time.Millisecond), float64(rows)/elapsed.Seconds())
	for _, name := range names {
		fmt.Fprintf(cmd.OutOrStdout(), "%s: %d documents, %.2f cumulative RU, %.2f mean RU/doc", name, docs[name], ru[name], ru[name]/float64(docs[name]))
		if conflicts[name] > 0 {
			fmt.Fprintf(cmd.OutOrStdout(), ", %d conflicts", conflicts[name])
		}
		if throttles[name] > 0 {
			fmt.Fprintf(cmd.OutOrStdout(), ", %d throttles", throttles[name])
		}
		fmt.Fprintln(cmd.OutOrStdout())
	}
	fmt.Fprintf(cmd.OutOrStdout(), "logical bytes ingested: %d (%.3f GiB)\n", logicalBytes, float64(logicalBytes)/(1<<30))
}

func (s *ingestStats) conflictRUFor(name string) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conflictRU[name]
}
