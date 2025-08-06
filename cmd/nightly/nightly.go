package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/turbopuffer/tpuf-benchmark/gen/dbq"
	"github.com/turbopuffer/turbopuffer-go"
	"github.com/turbopuffer/turbopuffer-go/option"
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
	mysqlDsn = flag.String(
		"mysql-dsn",
		"",
		"The MySQL DSN to connect to and store results in (optional)",
	)
	queryRunCount = flag.Int(
		"query-run-count",
		100,
		"The number of times to run each query",
	)
	namespacePrefix = flag.String(
		"namespace-prefix",
		"nightly_",
		"The prefix to use for the namespace names",
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

	dbc, err := maybeConnectToMySQL(ctx)
	if err != nil {
		return fmt.Errorf("connecting to MySQL: %w", err)
	}
	if dbc != nil {
		defer dbc.Close()
		logger.Info("connected to mysql db, will write benchmark results there")
	}

	datasource := NewCohereWikipediaEmbeddings(logger)
	datasets, err := LoadAllDatasets(logger, datasource, *templatesDir)
	if err != nil {
		return fmt.Errorf("loading datasets: %w", err)
	}
	logger.Info("loaded datasets", slog.Int("count", len(datasets)))

	for _, ds := range datasets {
		logger.Info("running benchmark for dataset", slog.String("dataset", ds.Label))
		results, err := ds.RunBenchmark(ctx, logger, tpuf, *namespacePrefix, *queryRunCount)
		if err != nil {
			return fmt.Errorf("running benchmark for dataset %q: %w", ds.Label, err)
		}
		logger.Info("benchmark run complete", slog.String("dataset", ds.Label))
		if dbc != nil {
			start := time.Now()
			if err := recordResultsToMySQL(ctx, dbc, results); err != nil {
				return fmt.Errorf("recording results to MySQL: %w", err)
			}
			logger.Info(
				"recorded results to MySQL",
				slog.String("dataset", ds.Label),
				slog.Duration("took", time.Since(start)),
			)
		}
	}

	logger.Info("all benchmarks completed successfully, exiting")

	return nil
}
func recordResultsToMySQL(ctx context.Context, dbc *sql.DB, brr *BenchmarkRunResult) error {
	tx, err := dbc.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning MySQL transaction: %w", err)
	}
	defer tx.Rollback()
	queries := dbq.New(tx)

	dataset, err := queries.GetDatasetByLabel(ctx, brr.DatasetLabel)
	if err == sql.ErrNoRows {
		dataset = dbq.BenchmarkDataset{
			Label:     brr.DatasetLabel,
			CreatedAt: brr.Timestamp,
		}
		res, err := queries.CreateDataset(ctx, dbq.CreateDatasetParams{
			Label:     dataset.Label,
			CreatedAt: dataset.CreatedAt,
		})
		if err != nil {
			return fmt.Errorf("creating dataset %q: %w", dataset.Label, err)
		}
		dataset.ID, err = res.LastInsertId()
		if err != nil {
			return fmt.Errorf("getting last insert ID for dataset %q: %w", dataset.Label, err)
		}
	} else if err != nil {
		return fmt.Errorf("getting dataset %q: %w", brr.DatasetLabel, err)
	}

	for _, qr := range brr.QueryResults {
		tags, err := json.Marshal(qr.QueryTags)
		if err != nil {
			return fmt.Errorf("marshalling query tags for %q: %w", qr.QueryLabel, err)
		}

		query, err := queries.GetQueryByLabel(ctx, dbq.GetQueryByLabelParams{
			DatasetID: dataset.ID,
			Label:     qr.QueryLabel,
		})
		if err == sql.ErrNoRows {
			query = dbq.BenchmarkQuery{
				DatasetID: dataset.ID,
				Label:     qr.QueryLabel,
				CreatedAt: brr.Timestamp,
				Tags:      json.RawMessage(tags),
			}
			res, err := queries.CreateQuery(ctx, dbq.CreateQueryParams{
				DatasetID: query.DatasetID,
				Label:     query.Label,
				CreatedAt: query.CreatedAt,
				Tags:      query.Tags,
			})
			if err != nil {
				return fmt.Errorf(
					"creating query %q for dataset %q: %w",
					query.Label,
					dataset.Label,
					err,
				)
			}
			query.ID, err = res.LastInsertId()
			if err != nil {
				return fmt.Errorf("getting last insert ID for query %q: %w", query.Label, err)
			}
		} else if err != nil {
			return fmt.Errorf("getting query %q for dataset %q: %w", qr.QueryLabel, dataset.Label, err)
		}

		if !bytes.Equal(query.Tags, tags) {
			if err := queries.UpdateQueryTags(ctx, dbq.UpdateQueryTagsParams{
				ID:   query.ID,
				Tags: json.RawMessage(tags),
			}); err != nil {
				return fmt.Errorf(
					"updating query tags for %q: %w",
					qr.QueryLabel,
					err,
				)
			}
		}

		if err := queries.InsertQueryResult(ctx, dbq.InsertQueryResultParams{
			QueryID:   query.ID,
			Timestamp: brr.Timestamp,
			P0Ms:      qr.PercentileValues[p0].Milliseconds(),
			P25Ms:     qr.PercentileValues[p25].Milliseconds(),
			P50Ms:     qr.PercentileValues[p50].Milliseconds(),
			P75Ms:     qr.PercentileValues[p75].Milliseconds(),
			P90Ms:     qr.PercentileValues[p90].Milliseconds(),
			P95Ms:     qr.PercentileValues[p95].Milliseconds(),
			P99Ms:     qr.PercentileValues[p99].Milliseconds(),
			P100Ms:    qr.PercentileValues[p100].Milliseconds(),
		}); err != nil {
			return fmt.Errorf(
				"inserting query result for query %q: %w",
				qr.QueryLabel,
				err,
			)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing MySQL transaction: %w", err)
	}

	return nil
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

func maybeConnectToMySQL(ctx context.Context) (*sql.DB, error) {
	if *mysqlDsn == "" {
		return nil, nil
	}

	// For parsing timestamps into Go time.Time objects
	dsn := *mysqlDsn
	if !strings.Contains(dsn, "parseTime") {
		if !strings.Contains(dsn, "?") {
			dsn += "?"
		} else {
			dsn += "&"
		}
		dsn += "parseTime=true"
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening mysql connection: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("pinging mysql database: %w", err)
	}

	return db, nil
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
