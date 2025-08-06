package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/turbopuffer/tpuf-benchmark/pkg/template"
	"github.com/turbopuffer/turbopuffer-go"
	"golang.org/x/sync/errgroup"
)

type Dataset struct {
	Label    string            // Set via the file path
	Document *DocumentTemplate // Template for the document
	Queries  []Query           // List of queries
}

type DocumentTemplate struct {
	Template *template.Template // Parsed template
	Rows     int                // Number of rows to upsert
}

type Query struct {
	Label    string // Set via the file path
	Tags     map[string]string
	Template *template.Template // Parsed template
}

// LoadAllDatasets loads all datasets from the specified directory.
// Structured like:
// - dataset1/
//   - document.tmpl
//   - query1.tmpl
//   - ...
//
// - dataset2/
//   - ...
func LoadAllDatasets(
	logger *slog.Logger,
	datasource template.Datasource,
	dirPath string,
) ([]*Dataset, error) {
	var datasets []*Dataset
	dirContents, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, fmt.Errorf("reading datasets directory %q: %w", dirPath, err)
	}
	for _, entry := range dirContents {
		fp := filepath.Join(dirPath, entry.Name())
		if !entry.IsDir() {
			logger.Warn(
				"skipping non-directory in datasets directory",
				slog.String("name", entry.Name()),
				slog.String("path", fp),
			)
			continue
		}
		ds, err := LoadDatasetFromDir(logger, datasource, fp)
		if err != nil {
			return nil, fmt.Errorf("loading dataset from %q: %w", fp, err)
		}
		datasets = append(datasets, ds)
	}
	return datasets, nil
}

// LoadDatasetFromDir loads a dataset from the specified directory.
// It reads all template files, parses them, and extracts the document template and queries.
// The directory must contain a document template named "document.tmpl" and at least one query template.
func LoadDatasetFromDir(
	logger *slog.Logger,
	datasource template.Datasource,
	dirPath string,
) (*Dataset, error) {
	dset := &Dataset{
		Label:    filepath.Base(dirPath),
		Document: nil,
		Queries:  nil,
	}

	dirContents, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, fmt.Errorf("reading dataset directory %q: %w", dirPath, err)
	}

	for _, entry := range dirContents {
		fp := filepath.Join(dirPath, entry.Name())

		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".tmpl") {
			logger.Warn(
				"skipping non-template file in dataset directory",
				slog.String("path", fp),
			)
			continue
		}

		contents, err := os.ReadFile(fp)
		if err != nil {
			return nil, fmt.Errorf("reading template file %q: %w", fp, err)
		}

		qlabel := strings.TrimSuffix(entry.Name(), ".tmpl")

		parsed, err := template.Parse(
			datasource,
			fmt.Sprintf("%s_%s", dset.Label, qlabel),
			string(contents),
		)
		if err != nil {
			return nil, fmt.Errorf("parsing template %q: %w", qlabel, err)
		}

		// Special handling for the document template
		if qlabel == "document" {
			if !parsed.HasAllTemplates("row", "document") {
				return nil, fmt.Errorf(
					"document template %q must define 'row' and 'document' templates",
					qlabel,
				)
			}
			rows, err := extractRowsTag(parsed)
			if err != nil {
				return nil, fmt.Errorf(
					"extracting 'rows' tag from document template %q: %w",
					qlabel,
					err,
				)
			}
			dset.Document = &DocumentTemplate{
				Template: parsed,
				Rows:     rows,
			}
			continue
		}

		// Otherwise, this is a query template.
		dset.Queries = append(dset.Queries, Query{
			Label:    qlabel,
			Template: parsed,
			Tags:     parsed.Tags(),
		})
	}

	if dset.Document == nil {
		return nil, fmt.Errorf("dataset %q does not contain a document template", dset.Label)
	} else if len(dset.Queries) == 0 {
		return nil, fmt.Errorf("dataset %q does not contain any query templates", dset.Label)
	}

	return dset, nil
}

// Extract the `rows` tag from the document template, parsing it into an integer.
func extractRowsTag(tmpl *template.Template) (int, error) {
	rowStr, ok := tmpl.Tag("rows")
	if !ok {
		return 0, errors.New("template does not have a 'rows' tag")
	}
	parsed, err := strconv.Atoi(rowStr)
	if err != nil {
		return 0, fmt.Errorf("invalid 'rows' tag value %q: %w", rowStr, err)
	} else if parsed <= 0 {
		return 0, fmt.Errorf("invalid 'rows' tag value %q: must be a positive integer", rowStr)
	}
	return parsed, nil
}

// UpsertRowsTo inserts the rows defined in the document template into the specified namespace.
// Returns the time taken to upsert the rows and the distance metric in use.
func (dt *DocumentTemplate) UpsertRowsTo(
	ctx context.Context,
	logger *slog.Logger,
	ns turbopuffer.Namespace,
) (time.Duration, turbopuffer.DistanceMetric, error) {
	writeParams, _, err := template.Render[turbopuffer.NamespaceWriteParams](
		dt.Template,
		"document",
	)
	if err != nil {
		return 0, "", fmt.Errorf("rendering document template: %w", err)
	}
	metric := writeParams.DistanceMetric // Used for waiting for indexing later, returned

	var (
		rows      []turbopuffer.RowParam
		rowsBytes uint
		eg        = new(errgroup.Group)
	)
	writeRows := func() {
		if len(rows) == 0 {
			return
		}
		params := writeParams
		size := rowsBytes
		params.UpsertRows = rows
		rows = nil
		rowsBytes = 0
		eg.Go(func() error {
			logger.Debug(
				"writing to namespace",
				slog.Uint64("size", uint64(size)),
			)
			if _, err := ns.Write(ctx, params); err != nil {
				return fmt.Errorf("upserting rows: %w", err)
			}
			return nil
		})
	}

	// TODO row rendering is CPU intensive, consider splitting across multiple goroutines
	start := time.Now()
	for i := 0; i < dt.Rows; i++ {
		row, sz, err := template.Render[turbopuffer.RowParam](dt.Template, "row")
		if err != nil {
			return 0, metric, fmt.Errorf("rendering document template for row %d: %w", i, err)
		}
		rowsBytes += sz
		rows = append(rows, row)
		if rowsBytes >= 128*1024*1024 { // 128 MiB
			writeRows()
		}
	}
	writeRows() // Write any remaining rows

	if err := eg.Wait(); err != nil {
		return 0, metric, fmt.Errorf("waiting for document upsert: %w", err)
	}

	return time.Since(start), metric, nil
}

// Deletes all the documents for a given namespace.
// If the namespace doesn't exist, no-op.
func clearNamespace(ctx context.Context, ns turbopuffer.Namespace) error {
	if _, err := ns.DeleteAll(ctx, turbopuffer.NamespaceDeleteAllParams{}); err != nil {
		var tpufErr *turbopuffer.Error
		if errors.As(err, &tpufErr) && tpufErr.StatusCode == 404 {
			return nil // No-op; namespace doesn't exist
		}
		return fmt.Errorf("deleting namespace: %w", err)
	}
	return nil
}

type BenchmarkRunResult struct {
	DatasetLabel   string
	Timestamp      time.Time
	UpsertDuration time.Duration
	UpsertRows     int
	QueryResults   []*DatasetQueryResult
}

// RunBenchmark clears the namespace, upserts the document rows, and runs all queries.
// Returns the benchmark run results, including the time taken to upsert rows and the results of each query.
func (d *Dataset) RunBenchmark(
	ctx context.Context,
	logger *slog.Logger,
	tpuf *turbopuffer.Client,
	namespacePrefix string,
	runs int,
) (*BenchmarkRunResult, error) {
	if runs <= 0 {
		return nil, fmt.Errorf("number of runs must be positive, got %d", runs)
	}
	logger = logger.With(
		slog.String("dataset", d.Label),
	)

	brr := &BenchmarkRunResult{
		DatasetLabel:   d.Label,
		Timestamp:      time.Now(),
		UpsertRows:     d.Document.Rows,
		UpsertDuration: 0,                                           // Set below
		QueryResults:   make([]*DatasetQueryResult, len(d.Queries)), // Set below
	}

	namespace := fmt.Sprintf("%s%s", namespacePrefix, d.Label)
	if err := clearNamespace(ctx, tpuf.Namespace(namespace)); err != nil {
		return nil, fmt.Errorf("clearing namespace %q: %w", namespace, err)
	}
	logger.Info("cleared namespace", slog.String("namespace", namespace))

	upsertDuration, distMetric, err := d.Document.UpsertRowsTo(
		ctx,
		logger,
		tpuf.Namespace(namespace),
	)
	if err != nil {
		return nil, fmt.Errorf("upserting document rows to namespace %q: %w", namespace, err)
	}
	brr.UpsertDuration = upsertDuration
	logger.Info("upserted document rows",
		slog.String("namespace", namespace),
		slog.Duration("duration", upsertDuration),
		slog.String("distance_metric", string(distMetric)),
	)

	if err := waitForIndexing(ctx, logger, tpuf, namespace, distMetric); err != nil {
		return nil, fmt.Errorf("waiting for indexing in namespace %q: %w", namespace, err)
	}
	logger.Info("indexing complete for namespace", slog.String("namespace", namespace))

	for i, query := range d.Queries {
		l := logger.With(slog.String("query", query.Label), slog.Int("run", runs))
		l.Info("running query")
		result, err := query.Run(ctx, tpuf, namespace, runs)
		if err != nil {
			return nil, fmt.Errorf(
				"running query %q against namespace %q: %w",
				query.Label,
				namespace,
				err,
			)
		}
		brr.QueryResults[i] = result
		fields := make([]any, numPercentileValues)
		for i, p := range allPercentiles() {
			fields[i] = slog.Duration(
				fmt.Sprintf("p%.0f", p.asFloat()*100),
				result.PercentileValues[i],
			)
		}
		l.Info("query run complete", fields...)
	}

	return brr, nil
}

// DatasetQueryResult holds the results of a query executed `runs` times against a namespace.
type DatasetQueryResult struct {
	QueryLabel       string
	QueryTags        map[string]string
	Runs             int
	PercentileValues [numPercentileValues]time.Duration
}

// Run executes the query `runs` times against the specified namespace.
// Returns the results of the query runs, including the time taken for each percentile.
func (q *Query) Run(
	ctx context.Context,
	tpuf *turbopuffer.Client,
	namespace string,
	runs int,
) (*DatasetQueryResult, error) {
	if runs <= 0 {
		return nil, fmt.Errorf("number of runs must be positive, got %d", runs)
	}

	durations := make([]time.Duration, runs)
	for i := range runs {
		query, err := template.RenderJSON(q.Template, "")
		if err != nil {
			return nil, fmt.Errorf("rendering query template %q: %w", q.Label, err)
		}
		var qr turbopuffer.NamespaceQueryResponse
		if err := tpuf.Execute(
			ctx,
			"POST",
			fmt.Sprintf("/v2/namespaces/%s/query", namespace),
			query,
			&qr,
		); err != nil {
			return nil, fmt.Errorf(
				"executing query %q against namespace %s: %w",
				q.Label,
				namespace,
				err,
			)
		}
		durations[i] = time.Duration(qr.Performance.QueryExecutionMs) * time.Millisecond
	}

	slices.Sort(durations)
	var percentileValues [numPercentileValues]time.Duration
	for i, p := range allPercentiles() {
		pf := p.asFloat()
		value, err := percentile(durations, pf)
		if err != nil {
			return nil, fmt.Errorf("calculating percentile %f: %w", pf, err)
		}
		percentileValues[i] = value
	}

	return &DatasetQueryResult{
		QueryLabel:       q.Label,
		QueryTags:        q.Tags,
		Runs:             runs,
		PercentileValues: percentileValues,
	}, nil
}

// Computes the specified percentile from the durations slice.
func percentile(durations []time.Duration, p float64) (time.Duration, error) {
	if p < 0 || p > 1 {
		return 0, fmt.Errorf("percentile must be between 0 and 1, got %f", p)
	} else if len(durations) == 0 {
		return 0, errors.New("cannot calculate percentile of empty durations slice")
	}
	switch p {
	case 0:
		return durations[0], nil
	case 1:
		return durations[len(durations)-1], nil
	default:
	}
	idx := p * float64(len(durations))
	if idx == float64(int64(idx)) {
		return durations[int(idx-1)], nil
	} else if idx > 1 {
		i := int(idx)
		return mean(durations[i-1], durations[i]), nil
	}
	return 0, fmt.Errorf("invalid percentile index %f", idx)
}

// Computes the mean of the given durations.
func mean(durations ...time.Duration) time.Duration {
	if len(durations) == 0 {
		panic("mean called with no durations")
	}
	var total time.Duration
	for _, d := range durations {
		total += d
	}
	return total / time.Duration(len(durations))
}

// Note: This function uses an internal API endpoint to wait for indexing to complete, which shouldn't
// be used in production code. It's _only_ for the purposes of this benchmark; we aren't trying to benchmark
// exhaustive search performance, but rather the performance of an indexed query.
func waitForIndexing(
	ctx context.Context,
	logger *slog.Logger,
	tpuf *turbopuffer.Client,
	ns string,
	metric turbopuffer.DistanceMetric,
) error {
	if metric == "" {
		metric = "euclidean" // No distance metric, likely vector implicit
	}
	marshaled, err := json.Marshal(map[string]string{
		"distance_metric": string(metric),
	})
	if err != nil {
		return fmt.Errorf("marshalling index params: %w", err)
	}

	for {
		resp := make(map[string]string)
		err = tpuf.Execute(
			ctx,
			"POST",
			fmt.Sprintf("/v1/namespaces/%s/index", ns),
			marshaled,
			&resp,
		)
		if err != nil {
			return fmt.Errorf("creating index for namespace %q: %w", ns, err)
		}
		message, ok := resp["message"]
		if !ok {
			return fmt.Errorf("/index response for namespace %q does not contain 'message'", ns)
		}
		if strings.Contains(message, "ignoring") {
			break // Indexing is done or not needed
		}

		logger.Info("waiting for indexing to complete", slog.String("namespace", ns))

		time.Sleep(time.Second * 30) // Wait before checking again
	}

	return nil
}

// Percentile represents array indices for different percentiles.
type Percentile int

const (
	p0 Percentile = iota
	p25
	p50
	p75
	p90
	p95
	p99
	p100
)

const numPercentileValues = p100 - p0 + 1

func (p Percentile) asFloat() float64 {
	switch p {
	case p0:
		return 0.0
	case p25:
		return 0.25
	case p50:
		return 0.5
	case p75:
		return 0.75
	case p90:
		return 0.9
	case p95:
		return 0.95
	case p99:
		return 0.99
	case p100:
		return 1.0
	default:
		panic(fmt.Sprintf("invalid percentile %d", p))
	}
}

func allPercentiles() iter.Seq2[int, Percentile] {
	return func(yield func(int, Percentile) bool) {
		for i := p0; i <= p100; i++ {
			if !yield(int(i), i) {
				return
			}
		}
	}
}
