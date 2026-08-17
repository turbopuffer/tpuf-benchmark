package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"
	"github.com/turbopuffer/tpuf-benchmark/pkg/datasource"
)

const (
	contentionQueryVectors  = 20
	contentionWriteRows     = 30000
	contentionMaxAttempts   = 20
	contentionAttemptGrace  = 10 * time.Second
	contentionFloat16Normal = float32(6.103515625e-05)
)

type contentionConfig struct {
	wps              string
	maxWriterThreads int
	concurrency      string
	duration         time.Duration
	database         string
	container        string
	cacheDir         string
}

type contentionWPS struct {
	label string
	max   bool
	rate  float64
}

type contentionRESTClient struct {
	endpoint     string
	key          []byte
	httpClient   *http.Client
	resourceLink string
	docsURL      string
	rangesURL    string
}

type contentionRESTResult struct {
	status     int
	ru         float64
	retryAfter time.Duration
	body       []byte
}

type contentionWriterStats struct {
	ok        atomic.Int64
	throttles atomic.Int64
	errors    atomic.Int64
	ruBits    atomic.Uint64
}

type contentionWriterSnapshot struct {
	ok        int64
	throttles int64
	errors    int64
	ru        float64
}

type contentionQueryStats struct {
	latencies []float64
	successRU []float64
	totalRU   float64
	throttles int64
	retries   int64
	giveups   int64
	errors    int64
}

type contentionStatusLogger struct {
	mu              sync.Mutex
	statuses        map[int]bool
	transportLogged bool
	out             io.Writer
}

type contentionPacer struct {
	mu       sync.Mutex
	next     time.Time
	interval time.Duration
}

func newContentionCommand() *cobra.Command {
	cfg := contentionConfig{}
	cmd := &cobra.Command{
		Use:   "contention",
		Short: "Benchmark query latency while generating write contention",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := commandContext()
			defer cancel()
			return runContention(ctx, cmd, cfg)
		},
	}
	cmd.Flags().StringVar(&cfg.wps, "wps", "0", "write rate: 0, a positive writes/second value, or max")
	cmd.Flags().IntVar(&cfg.maxWriterThreads, "max-writer-threads", 64, "maximum concurrent writer goroutines")
	cmd.Flags().StringVar(&cfg.concurrency, "conc", "1,4,8,16", "comma-separated query concurrency levels")
	cmd.Flags().DurationVar(&cfg.duration, "duration", 60*time.Second, "duration of each query concurrency cell")
	cmd.Flags().StringVar(&cfg.database, "database", "tpufbench", "database name")
	cmd.Flags().StringVar(&cfg.container, "container", "vectors2", "container name")
	cmd.Flags().StringVar(&cfg.cacheDir, "cache-dir", datasetCacheDir(), "datasource cache directory")
	return cmd
}

func runContention(ctx context.Context, cmd *cobra.Command, cfg contentionConfig) error {
	wps, err := parseContentionWPS(cfg.wps)
	if err != nil {
		return err
	}
	concurrency, err := parseContentionConcurrency(cfg.concurrency)
	if err != nil {
		return err
	}
	if cfg.maxWriterThreads <= 0 {
		return errors.New("max-writer-threads must be positive")
	}
	if cfg.duration <= 0 {
		return errors.New("duration must be positive")
	}

	restClient, err := newContentionRESTClient(cfg.database, cfg.container)
	if err != nil {
		return err
	}
	partitionRangeID, err := restClient.partitionKeyRangeID(ctx)
	if err != nil {
		return err
	}

	fmt.Fprintf(cmd.OutOrStdout(), "contention %s/%s wps=%s conc=%v cell=%.0fs\n",
		cfg.database, cfg.container, wps.label, concurrency, cfg.duration.Seconds())

	fmt.Fprintln(cmd.ErrOrStderr(), "loading query vectors...")
	queryRows, err := datasource.CohereMSMarcoQueryRows(ctx, datasource.Config{CacheDir: cfg.cacheDir, ParseConcurrency: 1})
	if err != nil {
		return fmt.Errorf("loading MSMarco queries: %w", err)
	}
	if len(queryRows) < contentionQueryVectors {
		return fmt.Errorf("need %d query vectors, dataset contains %d", contentionQueryVectors, len(queryRows))
	}
	queryVectors := make([][]float32, contentionQueryVectors)
	for i := range contentionQueryVectors {
		queryVectors[i] = queryRows[i].Vector
		sanitizeContentionVector(queryVectors[i])

	}
	queryBodies, err := marshalContentionQueryBodies(queryVectors)
	if err != nil {
		return err
	}

	var writeVectors [][]float32
	if wps.max || wps.rate > 0 {
		fmt.Fprintf(cmd.ErrOrStderr(), "loading %d write vectors from first corpus shard...\n", contentionWriteRows)
		writeVectors, err = loadContentionWriteVectors(ctx, cfg.cacheDir)
		if err != nil {
			return err
		}
	}

	logger := &contentionStatusLogger{statuses: make(map[int]bool), out: cmd.ErrOrStderr()}
	writerCtx, stopWriters := context.WithCancel(ctx)
	writerStats := &contentionWriterStats{}
	var writerGroup sync.WaitGroup
	if wps.max || wps.rate > 0 {
		startContentionWriters(writerCtx, &writerGroup, restClient, logger, writerStats, writeVectors, wps, cfg.maxWriterThreads)
	}
	defer func() {
		stopWriters()
		writerGroup.Wait()
	}()

	if wps.max || wps.rate > 0 {
		fmt.Fprintln(cmd.ErrOrStderr(), "letting writers settle 15s...")
		if err := waitContentionContext(ctx, 15*time.Second); err != nil {
			return err
		}
	}

	for _, conc := range concurrency {
		queryStats, elapsed, before, after, err := runContentionQueryCellWithWriters(ctx, restClient, logger, partitionRangeID, queryBodies, conc, cfg.duration, writerStats)
		if err != nil {
			return err
		}
		printContentionCell(cmd, wps.label, conc, elapsed, queryStats, before, after)
	}
	return nil
}

func parseContentionWPS(value string) (contentionWPS, error) {
	value = strings.TrimSpace(value)
	if value == "max" {
		return contentionWPS{label: "max", max: true}, nil
	}
	rate, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsNaN(rate) || math.IsInf(rate, 0) || rate < 0 {
		return contentionWPS{}, fmt.Errorf("wps must be 0, a non-negative number, or max, got %q", value)
	}
	return contentionWPS{label: value, rate: rate}, nil
}

func parseContentionConcurrency(value string) ([]int, error) {
	var levels []int
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		level, err := strconv.Atoi(part)
		if err != nil || level <= 0 {
			return nil, fmt.Errorf("conc must be a comma-separated list of positive integers, got %q", value)
		}
		levels = append(levels, level)
	}
	if len(levels) == 0 {
		return nil, errors.New("conc must contain at least one concurrency level")
	}
	return levels, nil
}

func newContentionRESTClient(database, container string) (*contentionRESTClient, error) {
	endpoint := strings.TrimRight(os.Getenv("COSMOS_ENDPOINT"), "/")
	if endpoint == "" {
		return nil, errors.New("COSMOS_ENDPOINT environment variable must be provided")
	}
	if _, err := url.ParseRequestURI(endpoint); err != nil {
		return nil, fmt.Errorf("invalid COSMOS_ENDPOINT: %w", err)
	}
	encodedKey := os.Getenv("COSMOS_KEY")
	if encodedKey == "" {
		return nil, errors.New("COSMOS_KEY environment variable must be provided")
	}
	key, err := base64.StdEncoding.DecodeString(encodedKey)
	if err != nil {
		return nil, fmt.Errorf("COSMOS_KEY must be a base64-encoded master key: %w", err)
	}
	resourceLink := "dbs/" + database + "/colls/" + container
	urlLink := "dbs/" + url.PathEscape(database) + "/colls/" + url.PathEscape(container)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 256
	transport.MaxIdleConnsPerHost = 256
	return &contentionRESTClient{
		endpoint: endpoint,
		key:      key,
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   60 * time.Second,
		},
		resourceLink: resourceLink,
		docsURL:      endpoint + "/" + urlLink + "/docs",
		rangesURL:    endpoint + "/" + urlLink + "/pkranges",
	}, nil
}

func (c *contentionRESTClient) authorization(method, resourceType, resourceLink, date string) string {
	payload := strings.ToLower(method) + "\n" + strings.ToLower(resourceType) + "\n" + resourceLink + "\n" + strings.ToLower(date) + "\n\n"
	mac := hmac.New(sha256.New, c.key)
	_, _ = mac.Write([]byte(payload))
	signature := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	return url.QueryEscape("type=master&ver=1.0&sig=" + signature)
}

func (c *contentionRESTClient) request(ctx context.Context, method, requestURL, resourceType string, headers http.Header, body []byte) (contentionRESTResult, error) {
	date := time.Now().UTC().Format(http.TimeFormat)
	req, err := http.NewRequestWithContext(ctx, method, requestURL, bytes.NewReader(body))
	if err != nil {
		return contentionRESTResult{}, err
	}
	req.Header.Set("x-ms-date", date)
	req.Header.Set("x-ms-version", "2020-07-15")
	req.Header.Set("Authorization", c.authorization(method, resourceType, c.resourceLink, date))
	for name, values := range headers {
		for _, value := range values {
			req.Header.Add(name, value)
		}
	}
	response, err := c.httpClient.Do(req)
	if err != nil {
		return contentionRESTResult{}, err
	}
	defer response.Body.Close()
	responseBody, readErr := io.ReadAll(response.Body)
	result := contentionRESTResult{
		status:     response.StatusCode,
		ru:         parseContentionRU(response.Header.Get("x-ms-request-charge")),
		retryAfter: parseContentionRetryAfter(response.Header.Get("x-ms-retry-after-ms")),
		body:       responseBody,
	}
	if readErr != nil {
		return result, readErr
	}
	return result, nil
}

func (c *contentionRESTClient) partitionKeyRangeID(ctx context.Context) (string, error) {
	result, err := c.request(ctx, http.MethodGet, c.rangesURL, "pkranges", nil, nil)
	if err != nil {
		return "", fmt.Errorf("getting partition key ranges: %w", err)
	}
	if result.status < 200 || result.status >= 300 {
		return "", fmt.Errorf("getting partition key ranges: status %d: %s", result.status, contentionBodySummary(result.body))
	}
	var response struct {
		Ranges []struct {
			ID string `json:"id"`
		} `json:"PartitionKeyRanges"`
	}
	if err := json.Unmarshal(result.body, &response); err != nil {
		return "", fmt.Errorf("decoding partition key ranges: %w", err)
	}
	if len(response.Ranges) != 1 {
		return "", fmt.Errorf("contention requires a single-partition container; found %d partition key ranges", len(response.Ranges))
	}
	if response.Ranges[0].ID == "" {
		return "", errors.New("partition key range response contained an empty id")
	}
	return response.Ranges[0].ID, nil
}

func (c *contentionRESTClient) query(ctx context.Context, partitionRangeID string, body []byte) (contentionRESTResult, error) {
	headers := make(http.Header)
	headers.Set("Content-Type", "application/query+json")
	headers.Set("x-ms-documentdb-isquery", "True")
	headers.Set("x-ms-documentdb-partitionkeyrangeid", partitionRangeID)
	headers.Set("x-ms-cosmos-supported-query-features", "OrderBy, Top, NonStreamingOrderBy")
	return c.request(ctx, http.MethodPost, c.docsURL, "docs", headers, body)
}

func (c *contentionRESTClient) write(ctx context.Context, partitionKey string, body []byte) (contentionRESTResult, error) {
	headers := make(http.Header)
	headers.Set("Content-Type", "application/json")
	partitionKeyJSON, _ := json.Marshal([]string{partitionKey})
	headers.Set("x-ms-documentdb-partitionkey", string(partitionKeyJSON))
	return c.request(ctx, http.MethodPost, c.docsURL, "docs", headers, body)
}

func marshalContentionQueryBodies(vectors [][]float32) ([][]byte, error) {
	type parameter struct {
		Name  string    `json:"name"`
		Value []float32 `json:"value"`
	}
	type queryBody struct {
		Query      string      `json:"query"`
		Parameters []parameter `json:"parameters"`
	}
	bodies := make([][]byte, len(vectors))
	for i, vector := range vectors {
		body, err := json.Marshal(queryBody{
			Query:      "SELECT TOP 10 c.id FROM c ORDER BY VectorDistance(c.vector, @v)",
			Parameters: []parameter{{Name: "@v", Value: vector}},
		})
		if err != nil {
			return nil, fmt.Errorf("marshalling query vector %d: %w", i, err)
		}
		bodies[i] = body
	}
	return bodies, nil
}

func loadContentionWriteVectors(ctx context.Context, cacheDir string) ([][]float32, error) {
	vectors := make([][]float32, 0, contentionWriteRows)
	rows := datasource.CohereMSMarcoFirstShardRows(ctx, datasource.Config{CacheDir: cacheDir, ParseConcurrency: 1})
	for row, err := range rows {
		if err != nil {
			return nil, fmt.Errorf("loading MSMarco corpus vectors: %w", err)
		}
		vectors = append(vectors, row.Vector)
		if len(vectors) == contentionWriteRows {
			break
		}
	}
	if len(vectors) != contentionWriteRows {
		return nil, fmt.Errorf("first MSMarco shard yielded only %d rows; need %d write vectors", len(vectors), contentionWriteRows)
	}
	return vectors, nil
}

func sanitizeContentionVector(vector []float32) {
	for i, value := range vector {
		absolute := float32(math.Abs(float64(value)))
		if absolute > 0 && absolute < contentionFloat16Normal {
			vector[i] = 0
		}
	}
}

func startContentionWriters(ctx context.Context, group *sync.WaitGroup, client *contentionRESTClient, logger *contentionStatusLogger, stats *contentionWriterStats, vectors [][]float32, wps contentionWPS, maxThreads int) {
	threads := maxThreads
	var pacer *contentionPacer
	if !wps.max {
		threads = min(maxThreads, int(math.Ceil(wps.rate)))
		interval := time.Duration(float64(time.Second) / wps.rate)
		if interval < time.Nanosecond {
			interval = time.Nanosecond
		}
		pacer = &contentionPacer{next: time.Now(), interval: interval}
	}
	var documentCounter atomic.Uint64
	runID := contentionRunID()
	for range threads {
		group.Add(1)
		go func() {
			defer group.Done()
			for {
				if pacer != nil {
					if err := pacer.wait(ctx); err != nil {
						return
					}
				} else if ctx.Err() != nil {
					return
				}
				counter := documentCounter.Add(1) - 1
				id := fmt.Sprintf("w%s-%d", runID, counter)
				vector := vectors[counter%uint64(len(vectors))]
				body, err := json.Marshal(struct {
					ID     string    `json:"id"`
					PK     string    `json:"pk"`
					Vector []float32 `json:"vector"`
				}{ID: id, PK: id, Vector: vector})
				if err != nil {
					stats.errors.Add(1)
					logger.logTransport(err)
					continue
				}
				for {
					result, err := client.write(ctx, id, body)
					stats.addRU(result.ru)
					if err != nil {
						if ctx.Err() != nil {
							return
						}
						stats.errors.Add(1)
						logger.logTransport(err)
						break
					}
					switch {
					case result.status >= 200 && result.status < 300:
						stats.ok.Add(1)
					case result.status == http.StatusTooManyRequests:
						stats.throttles.Add(1)
						if waitContentionContext(ctx, result.retryAfter) != nil {
							return
						}
						continue
					default:
						stats.errors.Add(1)
						logger.logStatus(result.status, result.body)
					}
					break
				}
			}
		}()
	}
}

func (p *contentionPacer) wait(ctx context.Context) error {
	p.mu.Lock()
	now := time.Now()
	deadline := p.next
	p.next = maxContentionTime(p.next, now).Add(p.interval)
	p.mu.Unlock()
	return waitContentionContext(ctx, time.Until(deadline))
}

func runContentionQueryCellWithWriters(ctx context.Context, client *contentionRESTClient, logger *contentionStatusLogger, partitionRangeID string, queryBodies [][]byte, concurrency int, duration time.Duration, writers *contentionWriterStats) (contentionQueryStats, time.Duration, contentionWriterSnapshot, contentionWriterSnapshot, error) {
	var writerBefore contentionWriterSnapshot
	if writers != nil {
		writerBefore = writers.snapshot()
	}
	cellStart := time.Now()
	cellEnd := cellStart.Add(duration)
	graceEnd := cellEnd.Add(contentionAttemptGrace)
	workerResults := make(chan contentionQueryStats, concurrency)
	var group sync.WaitGroup
	for range concurrency {
		group.Add(1)
		go func() {
			defer group.Done()
			stats := contentionQueryStats{}
			for time.Now().Before(cellEnd) && ctx.Err() == nil {
				start := time.Now()
				logicalRU := 0.0
				finished := false
				queryBody := queryBodies[rand.IntN(len(queryBodies))]
				for attempt := 1; attempt <= contentionMaxAttempts; attempt++ {
					if time.Now().After(graceEnd) {
						stats.giveups++
						finished = true
						break
					}
					if attempt > 1 {
						stats.retries++
					}
					attemptCtx, cancel := context.WithDeadline(ctx, graceEnd)
					result, err := client.query(attemptCtx, partitionRangeID, queryBody)
					cancel()
					logicalRU += result.ru
					stats.totalRU += result.ru
					if err != nil {
						if ctx.Err() != nil {
							finished = true
							break
						}
						if time.Now().After(graceEnd) || errors.Is(err, context.DeadlineExceeded) {
							stats.giveups++
						} else {
							stats.errors++
							logger.logTransport(err)
						}
						finished = true
						break
					}
					switch {
					case result.status >= 200 && result.status < 300:
						stats.latencies = append(stats.latencies, float64(time.Since(start))/float64(time.Millisecond))
						stats.successRU = append(stats.successRU, logicalRU)
						finished = true
					case result.status == http.StatusTooManyRequests:
						stats.throttles++
						if attempt == contentionMaxAttempts {
							stats.giveups++
							finished = true
							break
						}
						remaining := time.Until(graceEnd)
						if remaining <= 0 {
							stats.giveups++
							finished = true
							break
						}
						if err := waitContentionContext(ctx, min(result.retryAfter, remaining)); err != nil {
							finished = true
						}
					default:
						stats.errors++
						logger.logStatus(result.status, result.body)
						finished = true
					}
					if finished {
						break
					}
				}
				if !finished {
					stats.giveups++
				}
			}
			workerResults <- stats
		}()
	}
	group.Wait()
	var writerAfter contentionWriterSnapshot
	if writers != nil {
		writerAfter = writers.snapshot()
	}
	close(workerResults)
	elapsed := time.Since(cellStart)
	combined := contentionQueryStats{}
	for stats := range workerResults {
		combined.latencies = append(combined.latencies, stats.latencies...)
		combined.successRU = append(combined.successRU, stats.successRU...)
		combined.totalRU += stats.totalRU
		combined.throttles += stats.throttles
		combined.retries += stats.retries
		combined.giveups += stats.giveups
		combined.errors += stats.errors
	}
	if ctx.Err() != nil {
		return combined, elapsed, writerBefore, writerAfter, ctx.Err()
	}
	return combined, elapsed, writerBefore, writerAfter, nil
}

func printContentionCell(cmd *cobra.Command, wps string, concurrency int, elapsed time.Duration, queries contentionQueryStats, before, after contentionWriterSnapshot) {
	sort.Float64s(queries.latencies)
	seconds := elapsed.Seconds()
	qOK := len(queries.latencies)
	meanQueryRU := 0.0
	for _, ru := range queries.successRU {
		meanQueryRU += ru
	}
	if len(queries.successRU) > 0 {
		meanQueryRU /= float64(len(queries.successRU))
	}
	line := fmt.Sprintf("wps=%-4s conc=%2d  p50=%.1fms p95=%.1fms p99=%.1fms",
		wps, concurrency,
		contentionPercentile(queries.latencies, 50), contentionPercentile(queries.latencies, 95), contentionPercentile(queries.latencies, 99))
	line += fmt.Sprintf("  | queries/s=%.1f q_ru=%.1f", float64(qOK)/seconds, meanQueryRU)
	if queries.throttles > 0 {
		line += fmt.Sprintf(" q429=%d", queries.throttles)
	}
	if queries.giveups > 0 {
		line += fmt.Sprintf(" giveup=%d", queries.giveups)
	}
	if queries.errors > 0 {
		line += fmt.Sprintf(" err=%d", queries.errors)
	}
	totalRUPerSecond := queries.totalRU / seconds
	wOK := after.ok - before.ok
	if wOK > 0 || after.throttles > before.throttles {
		w429 := after.throttles - before.throttles
		wErr := after.errors - before.errors
		totalRUPerSecond += (after.ru - before.ru) / seconds
		meanWriteRU := 0.0
		if wOK > 0 {
			meanWriteRU = (after.ru - before.ru) / float64(wOK)
		}
		line += fmt.Sprintf("  | writes/s=%.1f w_ru=%.1f w429/s=%.1f", float64(wOK)/seconds, meanWriteRU, float64(w429)/seconds)
		if wErr > 0 {
			line += fmt.Sprintf(" w_err=%d", wErr)
		}
	}
	line += fmt.Sprintf("  | ru/s=%.0f", totalRUPerSecond)
	fmt.Fprintln(cmd.OutOrStdout(), line)
}

func contentionPercentile(sorted []float64, percentile int) float64 {
	if len(sorted) == 0 {
		return 0
	}
	index := int(math.Ceil(float64(percentile)/100*float64(len(sorted)))) - 1
	return sorted[max(0, index)]
}

func parseContentionRU(value string) float64 {
	ru, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0
	}
	return ru
}

func parseContentionRetryAfter(value string) time.Duration {
	milliseconds := 300.0
	if parsed, err := strconv.ParseFloat(value, 64); err == nil && parsed >= 0 {
		milliseconds = parsed
	}
	return max(50*time.Millisecond, time.Duration(milliseconds*float64(time.Millisecond)))
}

func (s *contentionWriterStats) addRU(ru float64) {
	for {
		oldBits := s.ruBits.Load()
		updated := math.Float64frombits(oldBits) + ru
		if s.ruBits.CompareAndSwap(oldBits, math.Float64bits(updated)) {
			return
		}
	}
}

func (s *contentionWriterStats) snapshot() contentionWriterSnapshot {
	return contentionWriterSnapshot{
		ok:        s.ok.Load(),
		throttles: s.throttles.Load(),
		errors:    s.errors.Load(),
		ru:        math.Float64frombits(s.ruBits.Load()),
	}
}

func (l *contentionStatusLogger) logStatus(status int, body []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.statuses[status] {
		return
	}
	l.statuses[status] = true
	fmt.Fprintf(l.out, "%s contention unexpected_status=%d body=%q\n", contentionTimestamp(), status, contentionBodySummary(body))
}

func (l *contentionStatusLogger) logTransport(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.transportLogged {
		return
	}
	l.transportLogged = true
	fmt.Fprintf(l.out, "%s contention transport_error=%q\n", contentionTimestamp(), err)
}

func contentionBodySummary(body []byte) string {
	const limit = 512
	text := strings.TrimSpace(string(body))
	if len(text) > limit {
		return text[:limit] + "..."
	}
	return text
}

func contentionRunID() string {
	var value [4]byte
	if _, err := cryptorand.Read(value[:]); err == nil {
		return fmt.Sprintf("%08x", binary.BigEndian.Uint32(value[:]))
	}
	return fmt.Sprintf("%08x", uint32(time.Now().UnixNano()))
}

func waitContentionContext(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func maxContentionTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return b
	}
	return a
}

func contentionTimestamp() string {
	return time.Now().Format("15:04:05")
}
