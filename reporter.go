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

	"github.com/turbopuffer/turbopuffer-go"
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
	performance *turbopuffer.QueryPerformance,
) error {
	r.Lock()
	defer r.Unlock()
	if err := r.queries.reportQuery(namespace, size, clientDuration, performance); err != nil {
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
	performance *turbopuffer.QueryPerformance,
) error {
	var (
		temp      = CacheTemperature(performance.CacheTemperature)
		latencies = qr.queryLatencies[temp]
		window    = len(latencies) - 1
	)
	latencies[window] = append(latencies[window], clientDuration)

	if qr.outputFile != nil {
		if err := qr.outputFile.Write([]string{
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

	return nil
}

func (qr *queryReporter) generateReport(allReportingPeriods bool, dur time.Duration) Report {
	report := make(Report)
	for temp, allSamples := range qr.queryLatencies {
		// Generate a combined set of latencies
		// By default, we only report on the last reporting window
		var samples latencies[time.Duration]
		if allReportingPeriods {
			samples = slices.Concat(allSamples...)
		} else {
			samples = allSamples[len(allSamples)-1]
		}
		if len(samples) == 0 {
			continue
		}
		slices.Sort(samples)

		var latencies strings.Builder
		latencies.WriteString(fmt.Sprintf("min=%dms", samples.percentile(0.0).Milliseconds()))
		for _, p := range []float64{10.0, 25.0, 50.0, 75.0, 90.0, 95.0, 99.0} {
			latencies.WriteString(fmt.Sprintf(", p%d=%dms", int(p), samples.percentile(p/100.0).Milliseconds()))
		}
		latencies.WriteString(fmt.Sprintf(", max=%dms", samples.percentile(1.0).Milliseconds()))

		report[fmt.Sprintf("%s_queries", temp)] = Report{
			"count":      int64(len(samples)),
			"throughput": float64(len(samples)) / dur.Seconds(),
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
	var samples latencies[UpsertStats]
	if allReportingPeriods {
		samples = slices.Concat(ur.upserts...)
	} else if len(ur.upserts) > 1 {
		samples = ur.upserts[len(ur.upserts)-1]
	}
	if len(samples) == 0 {
		return nil
	}
	slices.SortFunc(samples, func(a, b UpsertStats) int {
		return cmp.Compare(a.ClientTime, b.ClientTime)
	})

	var (
		totalDocs  int64
		totalBytes int64
	)
	for _, upsert := range samples {
		totalDocs += int64(upsert.NumDocuments)
		totalBytes += int64(upsert.TotalBytes)
	}

	var lats strings.Builder
	lats.WriteString(fmt.Sprintf("min=%dms", samples.percentile(0.0).ClientTime.Milliseconds()))
	for _, p := range []float64{10.0, 25.0, 50.0, 75.0, 90.0, 95.0, 99.0} {
		lats.WriteString(fmt.Sprintf(", p%d=%dms", int(p), samples.percentile(p/100.0).ClientTime.Milliseconds()))
	}
	lats.WriteString(fmt.Sprintf(", max=%dms", samples.percentile(1.0).ClientTime.Milliseconds()))

	return Report{
		"upserts": Report{
			"num_requests":  int64(len(samples)),
			"num_documents": totalDocs,
			"total_bytes":   totalBytes,
			"throughput":    float64(len(samples)) / dur.Seconds(),
			"latencies":     lats.String(),
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

type latencies[T any] []T

func (l latencies[T]) percentile(p float64) T {
	if len(l) == 0 {
		return *new(T)
	}
	var idx int
	switch p {
	case 0.0:
		idx = 0
	case 1.0:
		idx = len(l) - 1
	default:
		idx = int(float64(len(l)) * p)
	}
	return l[idx]
}
