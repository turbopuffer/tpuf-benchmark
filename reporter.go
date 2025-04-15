package main

import (
	"cmp"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Reporter is used to report on the outcome of a benchmark run
// by printing out the results to the console and a set of output
// files.
type Reporter struct {
	sync.Mutex
	outputDir       string
	printInterval   time.Duration
	firstReportTime time.Time
	lastReportTime  time.Time

	// Individual reporters
	queries *queryReporter
	upserts *upsertReporter
}

// Report is a JSON-serializable report of the benchmark run.
type Report map[string]any

// MergeOther merges another report into this one.
func (r Report) MergeOther(other Report) {
	for k, v := range other {
		if _, ok := r[k]; ok {
			panic(fmt.Sprintf("duplicate key in report: %s", k))
		}
		r[k] = v
	}
}

// PrintWithDepth prints a report with the given depth.
// Recursively prints sub-reports.
func (r Report) PrintWithDepth(depth int) {
	keys := make([]string, 0, len(r))
	for k := range r {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	for _, k := range keys {
		v := r[k]
		if sub, ok := v.(Report); ok {
			fmt.Printf("%s%s:\n", strings.Repeat("  ", depth), k)
			sub.PrintWithDepth(depth + 1)
		} else {
			fmt.Printf("%s%s: %v\n", strings.Repeat("  ", depth), k, v)
		}
	}
}

// NewReporter starts a new reporter.
func StartReporter() (*Reporter, error) {
	out := *outputDir
	if out != "" {
		if _, err := os.Stat(out); err == nil {
			return nil, fmt.Errorf("output directory already exists: %s", out)
		}
		if err := os.MkdirAll(out, 0755); err != nil {
			return nil, fmt.Errorf("failed to create output directory: %w", err)
		}
	} else {
		log.Printf("no -output-dir specified, results will not be written to disk")
	}

	queries, err := newQueryReporter(outputDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create query reporter: %w", err)
	}

	upserts, err := newUpsertReporter(outputDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create upsert reporter: %w", err)
	}

	return &Reporter{
		outputDir:      out,
		printInterval:  time.Second * 10,
		lastReportTime: time.Time{},
		queries:        queries,
		upserts:        upserts,
	}, nil
}

// Stop stops the reporter and flushes any remaining data.
// Should be called at the end of a benchmark run.
func (r *Reporter) Stop() error {
	r.Lock()
	defer r.Unlock()

	if err := r.queries.flush(); err != nil {
		return fmt.Errorf("failed to flush queries.csv: %w", err)
	}

	if err := r.upserts.flush(); err != nil {
		return fmt.Errorf("failed to flush upserts.csv: %w", err)
	}

	var (
		finalReport    = make(Report)
		overAllPeriods = true
		dur            = time.Since(r.firstReportTime)
	)
	if report := r.queries.generateReport(overAllPeriods, dur); len(report) > 0 {
		finalReport.MergeOther(report)
	}
	if report := r.upserts.generateReport(overAllPeriods, dur); len(report) > 0 {
		finalReport.MergeOther(report)
	}

	fmt.Println("")
	fmt.Printf("Cumulative report for the entire benchmark:\n")
	finalReport.PrintWithDepth(0)

	if r.outputDir != "" {
		f, err := os.Create(fmt.Sprintf("%s/report.json", r.outputDir))
		if err != nil {
			return fmt.Errorf("failed to create report.json: %w", err)
		}
		defer f.Close()
		if err := json.NewEncoder(f).Encode(finalReport); err != nil {
			return fmt.Errorf("failed to write report.json: %w", err)
		}
		log.Printf("results written to: %s", r.outputDir)
	}
	return nil
}

// ReportQuery reports the results of a single query.
func (r *Reporter) ReportQuery(
	namespace string,
	size int,
	clientDuration time.Duration,
	serverTiming *ServerTiming,
) error {
	r.Lock()
	defer r.Unlock()
	if err := r.queries.reportQuery(namespace, size, clientDuration, serverTiming); err != nil {
		return fmt.Errorf("failed to report query: %w", err)
	}
	r.maybePrintReport()
	return nil
}

// ReportUpsert reports the results of a single upsert.
func (r *Reporter) ReportUpsert(
	namespace string,
	numDocuments int,
	totalBytes int,
	clientDuration time.Duration,
) error {
	r.Lock()
	defer r.Unlock()
	if err := r.upserts.reportUpsert(namespace, numDocuments, totalBytes, clientDuration); err != nil {
		return fmt.Errorf("failed to report upsert: %w", err)
	}
	r.maybePrintReport()
	return nil
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

	var (
		report         = make(Report)
		overAllPeriods = false
		dur            = now.Sub(r.lastReportTime)
	)
	if qr := r.queries.generateReport(overAllPeriods, dur); len(qr) > 0 {
		report.MergeOther(qr)
	}
	if ur := r.upserts.generateReport(overAllPeriods, dur); len(ur) > 0 {
		report.MergeOther(ur)
	}

	runtime := now.Sub(r.firstReportTime).Round(time.Second)
	fmt.Println("")
	fmt.Printf("(%s) Report for the last %s:\n", runtime, r.printInterval)
	report.PrintWithDepth(0)

	r.lastReportTime = now
	r.queries.advanceReportingWindow()
	r.upserts.advanceReportingWindow()
}

// QueryReporter is used to report benchmark results for queries.
type queryReporter struct {
	queryLatencies map[CacheTemperature][][]time.Duration
	outputFile     *csv.Writer
}

func newQueryReporter(outputDir *string) (*queryReporter, error) {
	var outputFile *csv.Writer
	if outputDir != nil {
		f, err := os.Create(fmt.Sprintf("%s/queries.csv", *outputDir))
		if err != nil {
			return nil, fmt.Errorf("failed to create queries.csv: %w", err)
		}
		outputFile = csv.NewWriter(f)
		headers := []string{
			"namespace",
			"size",
			"cache_temperature",
			"client_duration_ms",
			"server_duration_ms",
			"exhaustive_count",
		}
		if err := outputFile.Write(headers); err != nil {
			return nil, fmt.Errorf("failed to write header to queries.csv: %w", err)
		}
	}
	ret := &queryReporter{
		queryLatencies: nil, // Initialized by advanceReportingWindow
		outputFile:     outputFile,
	}
	ret.advanceReportingWindow()
	return ret, nil
}

func (qr *queryReporter) advanceReportingWindow() {
	if qr.queryLatencies == nil {
		qr.queryLatencies = make(map[CacheTemperature][][]time.Duration)
	}
	for _, temp := range AllCacheTemperatures() {
		qr.queryLatencies[temp] = append(qr.queryLatencies[temp], nil)
	}
}

func (qr *queryReporter) reportQuery(
	namespace string,
	size int,
	clientDuration time.Duration,
	serverTiming *ServerTiming,
) error {
	var (
		temp      = *serverTiming.CacheTemperature
		latencies = qr.queryLatencies[temp]
		window    = len(latencies) - 1
	)
	latencies[window] = append(latencies[window], clientDuration)

	if qr.outputFile != nil {
		var exhaustiveCount int64
		if serverTiming.ExhaustiveCount != nil {
			exhaustiveCount = *serverTiming.ExhaustiveCount
		}
		if err := qr.outputFile.Write([]string{
			namespace,
			strconv.FormatInt(int64(size), 10),
			string(temp),
			strconv.FormatInt(clientDuration.Milliseconds(), 10),
			strconv.FormatUint(*serverTiming.ProcessingTimeMs, 10),
			strconv.FormatInt(exhaustiveCount, 10),
		}); err != nil {
			return fmt.Errorf("failed to write query to queries.csv: %w", err)
		}
	}

	return nil
}

func (qr *queryReporter) generateReport(allReportingPeriods bool, dur time.Duration) Report {
	report := make(Report)
	for temp, latencies := range qr.queryLatencies {
		// Generate a combined set of latencies
		// By default, we only report on the last reporting window
		var combined []time.Duration
		if allReportingPeriods {
			combined = combineLatencies(latencies)
		} else {
			combined = latencies[len(latencies)-1]
		}
		if len(combined) == 0 {
			continue
		}
		slices.Sort(combined)

		percentile := func(p float64) int64 {
			var idx int
			if p == 0.0 {
				idx = 0
			} else if p == 1.0 {
				idx = len(combined) - 1
			} else {
				idx = int(float64(len(combined)) * p)
			}
			return combined[idx].Milliseconds()
		}

		var latencies strings.Builder
		latencies.WriteString(fmt.Sprintf("min=%dms", percentile(0.0)))
		for _, p := range []float64{10.0, 25.0, 50.0, 75.0, 90.0, 95.0, 99.0} {
			latencies.WriteString(fmt.Sprintf(", p%d=%dms", int(p), percentile(p/100.0)))
		}
		latencies.WriteString(fmt.Sprintf(", max=%dms", percentile(1.0)))

		report[fmt.Sprintf("%s_queries", temp)] = Report{
			"count":      int64(len(combined)),
			"throughput": float64(len(combined)) / dur.Seconds(),
			"latencies":  latencies.String(),
		}
	}

	return report
}

func (qr *queryReporter) flush() error {
	if qr.outputFile != nil {
		qr.outputFile.Flush()
		if err := qr.outputFile.Error(); err != nil {
			return fmt.Errorf("failed to flush queries.csv: %w", err)
		}
	}
	return nil
}

// UpsertStats contains statistics about a single upsert operation.
type UpsertStats struct {
	NumDocuments int
	TotalBytes   int
	ClientTime   time.Duration
}

// UpsertReporter is used to report benchmark results for upserts.
type upsertReporter struct {
	upserts    [][]UpsertStats
	outputFile *csv.Writer
}

func newUpsertReporter(outputDir *string) (*upsertReporter, error) {
	var outputFile *csv.Writer
	if outputDir != nil {
		f, err := os.Create(fmt.Sprintf("%s/upserts.csv", *outputDir))
		if err != nil {
			return nil, fmt.Errorf("failed to create upserts.csv: %w", err)
		}
		outputFile = csv.NewWriter(f)
		headers := []string{
			"namespace",
			"num_documents",
			"request_bytes",
			"client_duration_ms",
		}
		if err := outputFile.Write(headers); err != nil {
			return nil, fmt.Errorf("failed to write header to upserts.csv: %w", err)
		}
	}
	ret := &upsertReporter{
		upserts:    [][]UpsertStats{nil},
		outputFile: outputFile,
	}
	return ret, nil
}

func (ur *upsertReporter) advanceReportingWindow() {
	ur.upserts = append(ur.upserts, nil)
}

func (ur *upsertReporter) reportUpsert(
	namespace string,
	numDocuments int,
	totalBytes int,
	clientDuration time.Duration,
) error {
	window := len(ur.upserts) - 1
	ur.upserts[window] = append(ur.upserts[window], UpsertStats{
		NumDocuments: numDocuments,
		TotalBytes:   totalBytes,
		ClientTime:   clientDuration,
	})

	if ur.outputFile != nil {
		if err := ur.outputFile.Write([]string{
			namespace,
			strconv.FormatInt(int64(numDocuments), 10),
			strconv.FormatInt(int64(totalBytes), 10),
			strconv.FormatInt(clientDuration.Milliseconds(), 10),
		}); err != nil {
			return fmt.Errorf("failed to write upsert to upserts.csv: %w", err)
		}
	}

	return nil
}

func (ur *upsertReporter) generateReport(allReportingPeriods bool, dur time.Duration) Report {
	var combined []UpsertStats
	if allReportingPeriods {
		combined = combineUpsertStats(ur.upserts)
	} else if len(ur.upserts) > 1 {
		combined = ur.upserts[len(ur.upserts)-1]
	}
	if len(combined) == 0 {
		return nil
	}
	slices.SortFunc(combined, func(a, b UpsertStats) int {
		return cmp.Compare(a.ClientTime, b.ClientTime)
	})

	percentile := func(p float64) int64 {
		var idx int
		if p == 0.0 {
			idx = 0
		} else if p == 1.0 {
			idx = len(combined) - 1
		} else {
			idx = int(float64(len(combined)) * p)
		}
		return combined[idx].ClientTime.Milliseconds()
	}

	var (
		totalDocs  int64
		totalBytes int64
	)
	for _, upsert := range combined {
		totalDocs += int64(upsert.NumDocuments)
		totalBytes += int64(upsert.TotalBytes)
	}

	var latencies strings.Builder
	latencies.WriteString(fmt.Sprintf("min=%dms", percentile(0.0)))
	for _, p := range []float64{10.0, 25.0, 50.0, 75.0, 90.0, 95.0, 99.0} {
		latencies.WriteString(fmt.Sprintf(", p%d=%dms", int(p), percentile(p/100.0)))
	}
	latencies.WriteString(fmt.Sprintf(", max=%dms", percentile(1.0)))

	return Report{
		"upserts": Report{
			"num_requests":  int64(len(combined)),
			"num_documents": totalDocs,
			"total_bytes":   totalBytes,
			"throughput":    float64(len(combined)) / dur.Seconds(),
			"latencies":     latencies.String(),
		},
	}
}

func (ur *upsertReporter) flush() error {
	if ur.outputFile != nil {
		ur.outputFile.Flush()
		if err := ur.outputFile.Error(); err != nil {
			return fmt.Errorf("failed to flush upserts.csv: %w", err)
		}
	}
	return nil
}

func combineLatencies(latencies [][]time.Duration) []time.Duration {
	if len(latencies) == 0 {
		return nil
	} else if len(latencies) == 1 {
		return latencies[0]
	}
	var total int
	for _, l := range latencies {
		total += len(l)
	}
	combined := make([]time.Duration, 0, total)
	for _, l := range latencies {
		combined = append(combined, l...)
	}
	return combined
}

func combineUpsertStats(stats [][]UpsertStats) []UpsertStats {
	if len(stats) == 0 {
		return nil
	} else if len(stats) == 1 {
		return stats[0]
	}
	var total int
	for _, s := range stats {
		total += len(s)
	}
	combined := make([]UpsertStats, 0, total)
	for _, s := range stats {
		combined = append(combined, s...)
	}
	return combined
}
