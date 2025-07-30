package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/turbopuffer/tpuf-benchmark/pkg/template"
	"github.com/turbopuffer/turbopuffer-go"
	"github.com/turbopuffer/turbopuffer-go/option"
	"golang.org/x/sync/errgroup"
)

var (
	flagBaseUrl = flag.String(
		"base-url",
		"gcp-us-central1.turbopuffer.com",
		"The base URL of the turbopuffer server",
	)
	flagAPIKey = flag.String(
		"api-key",
		"",
		"The API key to use for authentication with the turbopuffer server",
	)
	templatesDir = flag.String(
		"templates-dir",
		"templates/nightly",
		"The directory containing the templates to use for benchmarking",
	)
)

func main() {
	flag.Parse()
	logger := newLogger()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := run(ctx, logger); err != nil {
		logger.Error("top-level error", slog.Any("error", err))
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger) error {
	tpuf, err := newTurbopufferClient()
	if err != nil {
		return fmt.Errorf("creating turbopuffer client: %w", err)
	}

	datasource := template.NewRandomDatasource()

	// Load all datasets from the templates directory
	var datasets []*dataset
	tmplDirContents, err := os.ReadDir(*templatesDir)
	if err != nil {
		return fmt.Errorf("reading templates directory: %w", err)
	}
	for _, entry := range tmplDirContents {
		if !entry.IsDir() {
			logger.Warn(
				"skipping non-directory within templates directory",
				slog.String("name", entry.Name()),
				slog.String("path", *templatesDir+"/"+entry.Name()),
			)
			continue
		}
		fp := filepath.Join(*templatesDir, entry.Name())
		ds, err := loadDataset(logger, datasource, fp)
		if err != nil {
			return fmt.Errorf("loading dataset from %q: %w", fp, err)
		}
		datasets = append(datasets, ds)
	}
	logger.Info("loaded datasets", slog.Int("count", len(datasets)))

	for _, ds := range datasets {
		logger.Info("upserting documents for dataset", slog.String("name", ds.name))

		var (
			nsName = fmt.Sprintf("nightly-%s", ds.name)
			ns     = tpuf.Namespace(nsName)
		)

		// Clear the namespace if it exists
		if _, err := ns.DeleteAll(ctx, turbopuffer.NamespaceDeleteAllParams{}); err != nil {
			var tpufErr *turbopuffer.Error
			if errors.As(err, &tpufErr) && tpufErr.StatusCode == 404 {
				// No-op; namespace doesn't exist, no need to delete
			} else {
				return fmt.Errorf("deleting namespace %q: %w", nsName, err)
			}
		}

		// Upsert documents for the dataset
		took, err := ds.upsertDocuments(ctx, ns)
		if err != nil {
			return fmt.Errorf("upserting documents for dataset %q: %w", ds.name, err)
		}
		logger.Info("upserted documents for dataset",
			slog.String("name", ds.name),
			slog.Duration("took", took),
			slog.Int("rows", ds.rows),
		)

		// TODO wait for indexing to complete

		// Run all the queries for the dataset
		for i, query := range ds.queries {
			runs := 100
			logger.Info("running query against dataset",
				slog.String("name", ds.name),
				slog.Int("query_index", i),
				slog.String("namespace", nsName),
				slog.Int("runs", runs),
			)

			durations := make([]time.Duration, runs)
			for j := range runs {
				jsonQuery, err := template.RenderJSON(query)
				if err != nil {
					return fmt.Errorf(
						"rendering query template %d for dataset %q: %w",
						i,
						ds.name,
						err,
					)
				}

				var qr turbopuffer.NamespaceQueryResponse
				if err := tpuf.Execute(
					ctx,
					"POST",
					fmt.Sprintf("/v2/namespaces/%s/query", nsName),
					jsonQuery,
					&qr,
				); err != nil {
					return fmt.Errorf(
						"executing query %d against namespace %q: %w",
						i,
						nsName,
						err,
					)
				}

				durations[j] = time.Duration(qr.Performance.QueryExecutionMs) * time.Millisecond
			}

			slices.Sort(durations)
			percentile := func(p float64) time.Duration {
				if p < 0 || p > 1 {
					return 0
				}
				index := int(float64(len(durations)) * p)
				if index >= len(durations) {
					index = len(durations) - 1
				}
				return durations[index]
			}

			logger.Info("query results",
				slog.String("name", ds.name),
				slog.Int("query_index", i),
				slog.Int("runs", runs),
				slog.Duration("p0", percentile(0)),
				slog.Duration("p25", percentile(0.25)),
				slog.Duration("p50", percentile(0.5)),
				slog.Duration("p75", percentile(0.75)),
				slog.Duration("p90", percentile(0.9)),
				slog.Duration("p95", percentile(0.95)),
				slog.Duration("p99", percentile(0.99)),
				slog.Duration("p100", percentile(1)),
			)
		}
	}

	return nil
}

type dataset struct {
	name     string
	document *template.Template
	rows     int
	queries  []*template.Template
}

func loadDataset(
	logger *slog.Logger,
	datasource template.Datasource,
	path string,
) (*dataset, error) {
	dset := &dataset{
		name:     filepath.Base(path),
		document: nil,
		queries:  nil,
	}

	// Read all the template files in the dataset directory
	contents, err := os.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("reading dataset directory %q: %w", path, err)
	}
	for _, entry := range contents {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".tmpl") {
			logger.Warn(
				"skipping non-template file in dataset directory",
				slog.String("name", entry.Name()),
				slog.String("path", filepath.Join(path, entry.Name())),
			)
			continue
		}
		contents, err := os.ReadFile(filepath.Join(path, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("reading template file %q: %w", entry.Name(), err)
		}
		tmpl, err := template.Parse(
			datasource,
			fmt.Sprintf("%s_%s", dset.name, entry.Name()),
			string(contents),
		)
		if err != nil {
			return nil, fmt.Errorf("parsing template %q: %w", entry.Name(), err)
		}

		// If this is the document template, extract the 'rows' tag
		// and store it, otherwise add it to the queries list.
		// The document template must have a 'rows' tag.
		if entry.Name() == "document.tmpl" {
			rows, err := extractRowsTag(tmpl)
			if err != nil {
				return nil, fmt.Errorf(
					"extracting 'rows' tag from document template %q: %w",
					entry.Name(),
					err,
				)
			}
			dset.rows = rows
			dset.document = tmpl
			continue
		}

		dset.queries = append(dset.queries, tmpl)
	}

	if dset.document == nil {
		return nil, fmt.Errorf("dataset %q does not contain a document template", dset.name)
	} else if len(dset.queries) == 0 {
		return nil, fmt.Errorf("dataset %q does not contain any query templates", dset.name)
	}

	return dset, nil
}

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

func (ds *dataset) upsertDocuments(
	ctx context.Context,
	ns turbopuffer.Namespace,
) (time.Duration, error) {
	var (
		rows []turbopuffer.RowParam
		size uint
		eg   = new(errgroup.Group)
	)
	writeIfNonEmpty := func() {
		if len(rows) == 0 {
			return
		}
		batch := rows
		rows = nil
		size = 0
		eg.Go(func() error {
			if _, err := ns.Write(ctx, turbopuffer.NamespaceWriteParams{
				UpsertRows:     batch,
				DistanceMetric: turbopuffer.DistanceMetricEuclideanSquared, // TODO part of template
				Schema:         nil,                                        // TODO part of template
			}); err != nil {
				return fmt.Errorf("upserting rows: %w", err)
			}
			return nil
		})
	}

	start := time.Now()

	for i := 0; i < ds.rows; i++ {
		row, sz, err := template.Render[turbopuffer.RowParam](ds.document)
		if err != nil {
			return 0, fmt.Errorf("rendering document template for row %d: %w", i, err)
		}
		size += sz
		rows = append(rows, row)
		if size >= 128*1024*1024 { // 128 MiB
			writeIfNonEmpty()
		}
	}
	writeIfNonEmpty()

	if err := eg.Wait(); err != nil {
		return 0, fmt.Errorf("waiting for document upsert: %w", err)
	}

	return time.Since(start), nil
}

func newTurbopufferClient() (*turbopuffer.Client, error) {
	if *flagAPIKey == "" {
		flag.Usage()
		return nil, errors.New("missing required flag: -api-key")
	}

	client := turbopuffer.NewClient(
		option.WithBaseURL(*flagBaseUrl),
		option.WithAPIKey(*flagAPIKey),
		option.WithRegion("set-via-base-url"),
	)
	return &client, nil
}

var logLevels = map[string]slog.Level{
	"debug": slog.LevelDebug,
	"info":  slog.LevelInfo,
	"warn":  slog.LevelWarn,
	"error": slog.LevelError,
}

func newLogger() *slog.Logger {
	var leveler slog.Leveler
	if l, ok := logLevels[strings.ToLower(os.Getenv("LOG_LEVEL"))]; ok {
		leveler = l
	}
	var handler slog.Handler
	if localDev() {
		if leveler == nil {
			leveler = slog.LevelDebug
		}
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: leveler,
		})
	} else {
		handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: leveler,
		})
	}
	return slog.New(handler)
}

// TODO we can do better here
func localDev() bool {
	return runtime.GOOS == "darwin"
}
