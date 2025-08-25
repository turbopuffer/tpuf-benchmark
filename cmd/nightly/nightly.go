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
	"math"
	"net/http"
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
	flagSlackToken = flag.String(
		"slack-token",
		"",
		"The Slack token to use for sending notifications (optional)",
	)
	flagSlackChannelId = flag.String(
		"slack-channel-id",
		"",
		"The Slack channel ID to send notifications to (optional)",
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

		if dbc == nil {
			continue // TODO maybe do something useful when we don't have MySQL?
		}

		if *flagSlackToken != "" && *flagSlackChannelId != "" {
			diff, err := performanceDiffAgainstLastRun(ctx, dbc, results)
			if err != nil {
				return fmt.Errorf("computing performance diff against last run: %w", err)
			}
			if err := diff.printToSlack(ctx, results.DatasetLabel, *flagSlackToken, *flagSlackChannelId); err != nil {
				return fmt.Errorf("sending performance diff to slack: %w", err)
			}
			logger.Info("sent performance diff to slack", slog.String("dataset", ds.Label))
		}

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

	logger.Info("all benchmarks completed successfully, exiting")

	return nil
}

type queryPerformanceDiff struct {
	queryResult *DatasetQueryResult
	prev        map[Percentile]time.Duration
}

type performanceDiff struct {
	queries []queryPerformanceDiff
}

func (pd *performanceDiff) printToSlack(ctx context.Context, datasetLabel string, slackToken string, slackChannelId string) error {
	if len(pd.queries) == 0 {
		return nil
	}

	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("Nightly benchmark results (%s):\n", datasetLabel))

	for _, qpd := range pd.queries {
		line := fmt.Sprintf("• *%s*: P50=%dms, P95=%dms, P99=%dms",
			qpd.queryResult.QueryLabel,
			qpd.queryResult.PercentileValues[p50].Milliseconds(),
			qpd.queryResult.PercentileValues[p95].Milliseconds(),
			qpd.queryResult.PercentileValues[p99].Milliseconds())

		prev := qpd.prev
		if prev != nil {
			var (
				p50PctChange = float64(qpd.queryResult.PercentileValues[p50].Milliseconds()-prev[p50].Milliseconds()) / float64(prev[p50].Milliseconds()) * 100
				p95PctChange = float64(qpd.queryResult.PercentileValues[p95].Milliseconds()-prev[p95].Milliseconds()) / float64(prev[p95].Milliseconds()) * 100
				p99PctChange = float64(qpd.queryResult.PercentileValues[p99].Milliseconds()-prev[p99].Milliseconds()) / float64(prev[p99].Milliseconds()) * 100
			)
			line += fmt.Sprintf(" (vs prev: P50%+.1f%%, P95%+.1f%%, P99%+.1f%%)",
				math.Round(p50PctChange*10)/10,
				math.Round(p95PctChange*10)/10,
				math.Round(p99PctChange*10)/10,
			)
		}

		builder.WriteString(line + "\n")
	}

	payload, err := json.Marshal(map[string]any{
		"channel": slackChannelId,
		"text":    builder.String(),
	})
	if err != nil {
		return fmt.Errorf("marshaling slack payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", "https://slack.com/api/chat.postMessage", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("creating slack request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+slackToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("sending slack request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("non-200 response from slack: %d", resp.StatusCode)
	}

	return nil
}

// Computes the performance diff of this run, against the last run that was stored in MySQL.
// Returns a `performanceDiff` object, which can be formatted and sent to Slack etc.
func performanceDiffAgainstLastRun(ctx context.Context, dbc *sql.DB, brr *BenchmarkRunResult) (*performanceDiff, error) {
	queries := dbq.New(dbc)
	diff := &performanceDiff{}

	dataset, err := queries.GetDatasetByLabel(ctx, brr.DatasetLabel)
	if err != nil {
		if err == sql.ErrNoRows {
			for _, qr := range brr.QueryResults {
				diff.queries = append(diff.queries, queryPerformanceDiff{
					queryResult: qr,
					prev:        nil,
				})
			}
			return diff, nil
		}
		return nil, fmt.Errorf("getting dataset: %w", err)
	}

	for _, qr := range brr.QueryResults {
		pd := queryPerformanceDiff{
			queryResult: qr,
			prev:        nil,
		}
		query, err := queries.GetQueryByLabel(ctx, dbq.GetQueryByLabelParams{
			DatasetID: dataset.ID,
			Label:     qr.QueryLabel,
		})
		if err == sql.ErrNoRows {
			diff.queries = append(diff.queries, pd)
			continue
		} else if err != nil {
			return nil, fmt.Errorf("getting query %q: %w", qr.QueryLabel, err)
		}

		// Get the last run for this query.
		lastRun, err := queries.GetLastQueryResult(ctx, query.ID)
		if err == sql.ErrNoRows {
			diff.queries = append(diff.queries, pd)
			continue
		} else if err != nil {
			return nil, fmt.Errorf("getting last run for query %q: %w", qr.QueryLabel, err)
		}

		pd.prev = make(map[Percentile]time.Duration)
		pd.prev[p0] = time.Duration(lastRun.P0Ms) * time.Millisecond
		pd.prev[p25] = time.Duration(lastRun.P25Ms) * time.Millisecond
		pd.prev[p50] = time.Duration(lastRun.P50Ms) * time.Millisecond
		pd.prev[p75] = time.Duration(lastRun.P75Ms) * time.Millisecond
		pd.prev[p90] = time.Duration(lastRun.P90Ms) * time.Millisecond
		pd.prev[p95] = time.Duration(lastRun.P95Ms) * time.Millisecond
		pd.prev[p99] = time.Duration(lastRun.P99Ms) * time.Millisecond
		pd.prev[p100] = time.Duration(lastRun.P100Ms) * time.Millisecond

		diff.queries = append(diff.queries, pd)
	}

	return diff, nil
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
