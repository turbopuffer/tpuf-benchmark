package bench

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/RaduBerinde/tdigest"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/turbopuffer/tpuf-benchmark/pkg/output"
	"github.com/turbopuffer/turbopuffer-go"
)

// benchmarkingData holds structured benchmark report data for display. During
// periodic updates (isCumulative=false), both window and cumulative fields are
// populated. During the final report (isCumulative=true), only the window
// fields are populated with cumulative data.
type benchmarkingData struct {
	runtime      time.Duration
	windowSize   time.Duration
	isCumulative bool

	// Keyed by workload name, each containing per-temperature entries.
	queryReports map[string][]queryReportEntry
	// Keyed by workload name.
	upsertReports map[string]*upsertReportEntry

	// Cumulative data covering the entire benchmark run so far.
	// Populated alongside window data during periodic updates.
	cumulativeQueryReports  map[string][]queryReportEntry
	cumulativeUpsertReports map[string]*upsertReportEntry
}

// queryReportEntry holds report data for one cache temperature tier.
type queryReportEntry struct {
	cacheTemperature string
	count            int64
	throughput       float64
	latencies        latencyPercentiles
}

// upsertReportEntry holds report data for upserts.
type upsertReportEntry struct {
	numRequests  int64
	numDocuments int64
	totalBytes   int64
	throughput   float64
	latencies    latencyPercentiles
}

// latencyPercentiles holds latency percentile values in milliseconds.
type latencyPercentiles struct {
	min int64
	p10 int64
	p25 int64
	p50 int64
	p75 int64
	p90 int64
	p95 int64
	p99 int64
	max int64
}

// queryWorkloadMetrics holds per-cache-temperature histograms for a single
// named query workload.
type queryWorkloadMetrics struct {
	latencies map[CacheTemperature]*metricsReporter
}

func newQueryWorkloadMetrics() *queryWorkloadMetrics {
	m := &queryWorkloadMetrics{
		latencies: make(map[CacheTemperature]*metricsReporter),
	}
	for _, temp := range AllCacheTemperatures() {
		m.latencies[temp] = newMetricsReporter()
	}
	return m
}

// upsertWorkloadMetrics holds the histogram and doc/byte counters for a single
// named upsert workload.
type upsertWorkloadMetrics struct {
	latencies       *metricsReporter
	windowDocs      int64
	windowBytes     int64
	cumulativeDocs  int64
	cumulativeBytes int64
}

func newUpsertWorkloadMetrics() *upsertWorkloadMetrics {
	return &upsertWorkloadMetrics{
		latencies: newMetricsReporter(),
	}
}

// metricsReporter tracks latency samples using a t-digest for bounded-memory
// percentile estimation. It maintains two digests: one for the current
// reporting window and one cumulative (all-time).
type metricsReporter struct {
	window          tdigest.Builder
	cumulative      tdigest.Builder
	windowCount     int64
	cumulativeCount int64
}

const tdigestDelta = 128

// promHistogramBuckets defines 64 logarithmically-spaced bucket boundaries
// (0.5ms – 60s) for the Prometheus histogram metrics emitted by the Collector.
var promHistogramBuckets = prometheus.ExponentialBucketsRange(0.5, 60000, 128)

func newMetricsReporter() *metricsReporter {
	return &metricsReporter{
		window:     tdigest.MakeBuilder(tdigestDelta),
		cumulative: tdigest.MakeBuilder(tdigestDelta),
	}
}

// Record adds a single latency observation in milliseconds.
func (m *metricsReporter) Record(ms float64) {
	m.window.Add(ms, 1)
	m.cumulative.Add(ms, 1)
	m.windowCount++
	m.cumulativeCount++
}

// ResetWindow resets the window digest and count, leaving cumulative intact.
func (m *metricsReporter) ResetWindow() {
	m.window.Reset()
	m.windowCount = 0
}

// WindowPercentiles returns latencyPercentiles from the window digest.
func (m *metricsReporter) WindowPercentiles() latencyPercentiles {
	if m.windowCount == 0 {
		return latencyPercentiles{}
	}
	d := m.window.Digest()
	return percentilesFromDigest(&d)
}

// Percentiles returns latencyPercentiles from the cumulative digest.
func (m *metricsReporter) Percentiles() latencyPercentiles {
	if m.cumulativeCount == 0 {
		return latencyPercentiles{}
	}
	d := m.cumulative.Digest()
	return percentilesFromDigest(&d)
}

// percentilesFromDigest extracts the standard percentile set from a TDigest.
func percentilesFromDigest(d *tdigest.TDigest) latencyPercentiles {
	return latencyPercentiles{
		min: int64(d.Quantile(0.0)),
		p10: int64(d.Quantile(0.10)),
		p25: int64(d.Quantile(0.25)),
		p50: int64(d.Quantile(0.50)),
		p75: int64(d.Quantile(0.75)),
		p90: int64(d.Quantile(0.90)),
		p95: int64(d.Quantile(0.95)),
		p99: int64(d.Quantile(0.99)),
		max: int64(d.Quantile(1.0)),
	}
}

// Reporter is used to report on the outcome of a benchmark run
// by printing out the results to the console and a set of output
// files.
type Reporter struct {
	sync.Mutex
	outputDir       string
	printInterval   time.Duration
	firstReportTime time.Time
	lastReportTime  time.Time
	logger          *output.Logger

	// Query metrics: one set of per-temperature histograms per workload name.
	queryMetrics map[string]*queryWorkloadMetrics
	queriesCSV   *csv.Writer

	// Upsert metrics: one set of counters/histograms per workload name.
	upsertMetrics map[string]*upsertWorkloadMetrics
	upsertsCSV    *csv.Writer

	// Prometheus histogram metrics for latency tracking.
	promQueryHist  *prometheus.HistogramVec
	promUpsertHist *prometheus.HistogramVec
}

// StartReporter starts a new reporter.
func StartReporter(definitionName, outputDir string, logger *output.Logger) (*Reporter, error) {
	var err error
	out := outputDir
	if out == "" {
		if err := os.MkdirAll("results", 0755); err != nil {
			return nil, fmt.Errorf("failed to create results directory: %w", err)
		}
		out, err = os.MkdirTemp("results", definitionName+"_*")
		if err != nil {
			return nil, fmt.Errorf("failed to create output directory: %w", err)
		}
		logger.Detailf("no -output-dir specified, using temporary directory: %s", out)
	} else if out != "" {
		if _, err := os.Stat(out); err == nil {
			return nil, fmt.Errorf("output directory already exists: %s", out)
		}
		if err := os.MkdirAll(out, 0755); err != nil {
			return nil, fmt.Errorf("failed to create output directory: %w", err)
		}
	}

	r := &Reporter{
		outputDir:     out,
		printInterval: 10 * time.Second,
		logger:        logger,
		queryMetrics:  make(map[string]*queryWorkloadMetrics),
		upsertMetrics: make(map[string]*upsertWorkloadMetrics),
		promQueryHist: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "tpufbench_query_duration_ms",
			Help:    "Client-observed query latency in milliseconds.",
			Buckets: promHistogramBuckets,
		}, []string{"workload", "cache_temperature"}),
		promUpsertHist: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "tpufbench_upsert_duration_ms",
			Help:    "Client-observed upsert latency in milliseconds.",
			Buckets: promHistogramBuckets,
		}, []string{"workload"}),
	}
	prometheus.MustRegister(r.promQueryHist, r.promUpsertHist)

	// Create CSV output files.
	if out != "" {
		r.queriesCSV, err = createCSV(out, "queries.csv", []string{
			"workload", "namespace", "size", "cache_temperature",
			"client_duration_ms", "server_duration_ms", "exhaustive_count",
		})
		if err != nil {
			return nil, err
		}
		r.upsertsCSV, err = createCSV(out, "upserts.csv", []string{
			"workload", "namespace", "num_documents", "request_bytes", "client_duration_ms",
		})
		if err != nil {
			return nil, err
		}
	}

	return r, nil
}

// createCSV creates a CSV file with the given headers.
func createCSV(dir, name string, headers []string) (*csv.Writer, error) {
	f, err := os.Create(fmt.Sprintf("%s/%s", dir, name))
	if err != nil {
		return nil, fmt.Errorf("failed to create %s: %w", name, err)
	}
	w := csv.NewWriter(f)
	if err := w.Write(headers); err != nil {
		return nil, fmt.Errorf("failed to write header to %s: %w", name, err)
	}
	return w, nil
}

// getOrCreateQueryMetrics returns the queryWorkloadMetrics for the given
// workload name, creating it if necessary. Must be called with the lock held.
func (r *Reporter) getOrCreateQueryMetrics(workload string) *queryWorkloadMetrics {
	qm, ok := r.queryMetrics[workload]
	if !ok {
		qm = newQueryWorkloadMetrics()
		r.queryMetrics[workload] = qm
	}
	return qm
}

// getOrCreateUpsertMetrics returns the upsertWorkloadMetrics for the given
// workload name, creating it if necessary. Must be called with the lock held.
func (r *Reporter) getOrCreateUpsertMetrics(workload string) *upsertWorkloadMetrics {
	um, ok := r.upsertMetrics[workload]
	if !ok {
		um = newUpsertWorkloadMetrics()
		r.upsertMetrics[workload] = um
	}
	return um
}

// ReportQuery reports the results of a single query.
func (r *Reporter) ReportQuery(
	workload string,
	namespace string,
	size int,
	clientDuration time.Duration,
	performance *turbopuffer.QueryPerformance,
) error {
	r.Lock()
	defer r.Unlock()

	qm := r.getOrCreateQueryMetrics(workload)
	temp := CacheTemperature(performance.CacheTemperature)
	ms := float64(clientDuration.Milliseconds())
	qm.latencies[temp].Record(ms)
	r.promQueryHist.WithLabelValues(workload, string(temp)).Observe(ms)

	if r.queriesCSV != nil {
		if err := r.queriesCSV.Write([]string{
			workload,
			namespace,
			strconv.FormatInt(int64(size), 10),
			string(temp),
			strconv.FormatInt(clientDuration.Milliseconds(), 10),
			strconv.FormatInt(performance.ServerTotalMs, 10),
			strconv.FormatInt(performance.ExhaustiveSearchCount, 10),
		}); err != nil {
			return fmt.Errorf("failed to write query to queries.csv: %w", err)
		}
	}

	r.maybePrintReport()
	return nil
}

// ReportUpsert reports the results of a single upsert.
func (r *Reporter) ReportUpsert(
	workload string,
	namespace string,
	numDocuments int,
	totalBytes int,
	clientDuration time.Duration,
) error {
	r.Lock()
	defer r.Unlock()

	um := r.getOrCreateUpsertMetrics(workload)
	ms := float64(clientDuration.Milliseconds())
	um.latencies.Record(ms)
	r.promUpsertHist.WithLabelValues(workload).Observe(ms)
	um.windowDocs += int64(numDocuments)
	um.windowBytes += int64(totalBytes)
	um.cumulativeDocs += int64(numDocuments)
	um.cumulativeBytes += int64(totalBytes)

	if r.upsertsCSV != nil {
		if err := r.upsertsCSV.Write([]string{
			workload,
			namespace,
			strconv.FormatInt(int64(numDocuments), 10),
			strconv.FormatInt(int64(totalBytes), 10),
			strconv.FormatInt(clientDuration.Milliseconds(), 10),
		}); err != nil {
			return fmt.Errorf("failed to write upsert to upserts.csv: %w", err)
		}
	}

	r.maybePrintReport()
	return nil
}

// Stop stops the reporter and flushes any remaining data.
// Should be called at the end of a benchmark run.
func (r *Reporter) Stop() error {
	r.Lock()
	defer r.Unlock()

	if err := flushCSV(r.queriesCSV); err != nil {
		return fmt.Errorf("failed to flush queries.csv: %w", err)
	}
	if err := flushCSV(r.upsertsCSV); err != nil {
		return fmt.Errorf("failed to flush upserts.csv: %w", err)
	}

	dur := time.Since(r.firstReportTime)
	reportData := r.buildReportData(true, dur)
	r.logReport(reportData)

	if r.outputDir != "" {
		finalReport := r.generateJSONReport(dur)
		f, err := os.Create(fmt.Sprintf("%s/report.json", r.outputDir))
		if err != nil {
			return fmt.Errorf("failed to create report.json: %w", err)
		}
		defer f.Close()
		if err := json.NewEncoder(f).Encode(finalReport); err != nil {
			return fmt.Errorf("failed to write report.json: %w", err)
		}
		r.logger.Detailf("results written to: %s", r.outputDir)
	}
	return nil
}

func flushCSV(w *csv.Writer) error {
	if w == nil {
		return nil
	}
	w.Flush()
	return w.Error()
}

// buildReportData constructs structured report data for logging.
func (r *Reporter) buildReportData(allPeriods bool, dur time.Duration) benchmarkingData {
	data := benchmarkingData{
		runtime:       time.Since(r.firstReportTime).Round(time.Second),
		windowSize:    r.printInterval,
		isCumulative:  allPeriods,
		queryReports:  make(map[string][]queryReportEntry),
		upsertReports: make(map[string]*upsertReportEntry),
	}

	for workload, qm := range r.queryMetrics {
		var entries []queryReportEntry
		combined := tdigest.MakeBuilder(tdigestDelta)
		var combinedCount int64
		for _, temp := range AllCacheTemperatures() {
			m := qm.latencies[temp]
			entry := queryReportEntry{
				cacheTemperature: string(temp),
			}
			if allPeriods {
				if m.cumulativeCount > 0 {
					entry.count = m.cumulativeCount
					entry.throughput = float64(m.cumulativeCount) / dur.Seconds()
					entry.latencies = m.Percentiles()
					d := m.cumulative.Digest()
					combined.Merge(&d)
					combinedCount += m.cumulativeCount
				}
			} else {
				if m.windowCount > 0 {
					entry.count = m.windowCount
					entry.throughput = float64(m.windowCount) / dur.Seconds()
					entry.latencies = m.WindowPercentiles()
					d := m.window.Digest()
					combined.Merge(&d)
					combinedCount += m.windowCount
				}
			}
			entries = append(entries, entry)
		}
		if combinedCount > 0 {
			d := combined.Digest()
			entries = append(entries, queryReportEntry{
				cacheTemperature: "combined",
				count:            combinedCount,
				throughput:       float64(combinedCount) / dur.Seconds(),
				latencies:        percentilesFromDigest(&d),
			})
		}
		data.queryReports[workload] = entries
	}

	for workload, um := range r.upsertMetrics {
		if allPeriods {
			if um.latencies.cumulativeCount > 0 {
				data.upsertReports[workload] = &upsertReportEntry{
					numRequests:  um.latencies.cumulativeCount,
					numDocuments: um.cumulativeDocs,
					totalBytes:   um.cumulativeBytes,
					throughput:   float64(um.latencies.cumulativeCount) / dur.Seconds(),
					latencies:    um.latencies.Percentiles(),
				}
			}
		} else {
			if um.latencies.windowCount > 0 {
				data.upsertReports[workload] = &upsertReportEntry{
					numRequests:  um.latencies.windowCount,
					numDocuments: um.windowDocs,
					totalBytes:   um.windowBytes,
					throughput:   float64(um.latencies.windowCount) / dur.Seconds(),
					latencies:    um.latencies.WindowPercentiles(),
				}
			}
		}
	}

	return data
}

// maybePrintReport prints a report if the print interval has elapsed.
// Requires the lock to be held.
func (r *Reporter) maybePrintReport() {
	now := time.Now()
	if time.Since(r.lastReportTime) < r.printInterval {
		return
	} else if r.firstReportTime.IsZero() {
		r.firstReportTime = now
		r.lastReportTime = now
		return
	}

	windowDur := now.Sub(r.lastReportTime)
	totalDur := now.Sub(r.firstReportTime)
	reportData := r.buildReportData(false, windowDur)

	// Also build cumulative data so both windows are logged.
	cumulativeData := r.buildReportData(true, totalDur)
	reportData.cumulativeQueryReports = cumulativeData.queryReports
	reportData.cumulativeUpsertReports = cumulativeData.upsertReports

	r.logReport(reportData)
	r.lastReportTime = now

	// Advance the reporting window.
	for _, qm := range r.queryMetrics {
		for _, m := range qm.latencies {
			m.ResetWindow()
		}
	}
	for _, um := range r.upsertMetrics {
		um.latencies.ResetWindow()
		um.windowDocs = 0
		um.windowBytes = 0
	}
}

// logReport writes a formatted benchmark report through the logger.
func (r *Reporter) logReport(data benchmarkingData) {
	r.logger.WriteLines(func(p string, w io.Writer) {
		if data.isCumulative {
			fmt.Fprintf(w, "\n%s Cumulative report for the entire benchmark:\n", p)
		} else {
			fmt.Fprintf(w, "\n%s (%s) Report for the last %s:\n", p, data.runtime, data.windowSize)
		}

		logQueryReports(w, p, data.queryReports)
		logUpsertReports(w, p, data.upsertReports)

		// Print cumulative data alongside window data when available.
		if !data.isCumulative && (len(data.cumulativeQueryReports) > 0 || len(data.cumulativeUpsertReports) > 0) {
			fmt.Fprintf(w, "\n%s Cumulative (overall):\n", p)
			logQueryReports(w, p, data.cumulativeQueryReports)
			logUpsertReports(w, p, data.cumulativeUpsertReports)
		}
	})
}

func logQueryReports(w io.Writer, p string, reports map[string][]queryReportEntry) {
	for _, workload := range sortedKeys(reports) {
		fmt.Fprintf(w, "%s   [%s] queries:\n", p, workload)
		for _, qr := range reports[workload] {
			fmt.Fprintf(w, "%s     %s:\n", p, qr.cacheTemperature)
			fmt.Fprintf(w, "%s       count: %d\n", p, qr.count)
			fmt.Fprintf(w, "%s       throughput: %.1f queries/sec\n", p, qr.throughput)
			fmt.Fprintf(w, "%s       latencies: %s\n", p, formatLatencies(qr.latencies))
		}
	}
}

func logUpsertReports(w io.Writer, p string, reports map[string]*upsertReportEntry) {
	for _, workload := range sortedKeys(reports) {
		ur := reports[workload]
		if ur == nil {
			continue
		}
		fmt.Fprintf(w, "%s   [%s] upserts:\n", p, workload)
		fmt.Fprintf(w, "%s     num_requests: %d\n", p, ur.numRequests)
		fmt.Fprintf(w, "%s     num_documents: %d\n", p, ur.numDocuments)
		fmt.Fprintf(w, "%s     total_bytes: %d\n", p, ur.totalBytes)
		fmt.Fprintf(w, "%s     throughput: %.1f requests/sec\n", p, ur.throughput)
		fmt.Fprintf(w, "%s     latencies: %s\n", p, formatLatencies(ur.latencies))
	}
}

func formatLatencies(l latencyPercentiles) string {
	return fmt.Sprintf(
		"min=%dms, p50=%dms, p90=%dms, p99=%dms, max=%dms",
		l.min, l.p50, l.p90, l.p99, l.max,
	)
}

// sortedKeys returns the keys of a map in sorted order.
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// generateJSONReport builds the final JSON report from cumulative metrics.
func (r *Reporter) generateJSONReport(dur time.Duration) map[string]any {
	result := make(map[string]any)

	// Per-workload query reports.
	if len(r.queryMetrics) > 0 {
		queriesMap := make(map[string]any)
		for _, workload := range sortedKeys(r.queryMetrics) {
			qm := r.queryMetrics[workload]
			workloadMap := make(map[string]any)
			combined := newMetricsReporter()
			for _, temp := range AllCacheTemperatures() {
				m := qm.latencies[temp]
				if m.cumulativeCount == 0 {
					continue
				}
				workloadMap[string(temp)] = metricsToJSON(m, dur)
				d := m.cumulative.Digest()
				combined.cumulative.Merge(&d)
				combined.cumulativeCount += m.cumulativeCount
			}
			if combined.cumulativeCount > 0 {
				workloadMap["combined"] = metricsToJSON(combined, dur)
			}
			queriesMap[workload] = workloadMap
		}
		result["queries"] = queriesMap
	}

	// Per-workload upsert reports.
	if len(r.upsertMetrics) > 0 {
		upsertsMap := make(map[string]any)
		for _, workload := range sortedKeys(r.upsertMetrics) {
			um := r.upsertMetrics[workload]
			if um.latencies.cumulativeCount == 0 {
				continue
			}
			upsertJSON := metricsToJSON(um.latencies, dur)
			upsertJSON["num_documents"] = um.cumulativeDocs
			upsertJSON["total_bytes"] = um.cumulativeBytes
			upsertsMap[workload] = upsertJSON
		}
		result["upserts"] = upsertsMap
	}

	return result
}

// metricsToJSON converts a metricsReporter's cumulative data into a JSON-friendly map.
func metricsToJSON(m *metricsReporter, dur time.Duration) map[string]any {
	d := m.cumulative.Digest()
	var latencies strings.Builder
	latencies.WriteString(fmt.Sprintf("min=%dms", int64(d.Quantile(0.0))))
	for _, p := range []float64{0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99} {
		latencies.WriteString(fmt.Sprintf(", p%d=%dms", int(p*100), int64(d.Quantile(p))))
	}
	latencies.WriteString(fmt.Sprintf(", max=%dms", int64(d.Quantile(1.0))))
	return map[string]any{
		"count":      m.cumulativeCount,
		"throughput": float64(m.cumulativeCount) / dur.Seconds(),
		"latencies":  latencies.String(),
	}
}
