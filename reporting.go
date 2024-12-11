package main

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/narqo/psqr"
)

type report struct {
	query  *queryStats
	upsert *upsertStats
}

type currentWindow struct {
	startedAt time.Time
	queries   map[queryTemperature][]*queryStats
	upserts   []*upsertStats
}

func (cw *currentWindow) processIncomingReport(r report) {
	if r.query != nil {
		if cw.queries == nil {
			cw.queries = make(map[queryTemperature][]*queryStats)
		}
		cw.queries[r.query.temperature] = append(
			cw.queries[r.query.temperature],
			r.query,
		)
	}
	if r.upsert != nil {
		cw.upserts = append(cw.upserts, r.upsert)
	}
}

func (cw *currentWindow) constructLogMessages() []string {
	var (
		builder  strings.Builder
		messages []string
	)

	for temp, stats := range cw.queries {
		if len(stats) == 0 {
			continue
		}
		slices.SortFunc(stats, func(a, b *queryStats) int {
			return cmp.Compare(a.clientLatency, b.clientLatency)
		})
		percentile := func(p float32) time.Duration {
			idx := int(float32(len(stats)) * p / 100)
			return stats[idx].clientLatency
		}
		switch temp {
		case queryTemperatureCold:
			builder.WriteString("\x1b[36m")
		case queryTemperatureWarm:
			builder.WriteString("\x1b[33m")
		case queryTemperatureHot:
			builder.WriteString("\x1b[31m")
		default:
			panic("unreachable")
		}
		builder.WriteString(string(temp))
		builder.WriteString("\x1b[0m ")
		builder.WriteString(
			fmt.Sprintf(
				"queries (%d), latencies (ms): p25=%d, p50=%d, p75=%d, p90=%d, p99=%d\n",
				len(stats),
				percentile(25).Milliseconds(),
				percentile(50).Milliseconds(),
				percentile(75).Milliseconds(),
				percentile(90).Milliseconds(),
				percentile(99).Milliseconds(),
			),
		)
		messages = append(messages, builder.String())
		builder.Reset()
	}

	if len(cw.upserts) > 0 {
		slices.SortFunc(cw.upserts, func(a, b *upsertStats) int {
			return cmp.Compare(a.duration, b.duration)
		})
		var (
			sumUpserts  int64
			sumDuration time.Duration
		)
		for _, upsert := range cw.upserts {
			sumUpserts += int64(upsert.upserted)
			sumDuration += upsert.duration
		}
		percentile := func(p float32) time.Duration {
			idx := int(float32(len(cw.upserts)) * p / 100)
			return cw.upserts[idx].duration
		}
		builder.WriteString(
			fmt.Sprintf(
				"%d upserts (%d documents; %d wps), latencies (ms): p25=%d, p50=%d, p75=%d, p90=%d, p99=%d\n",
				len(cw.upserts),
				sumUpserts,
				int64(float64(sumUpserts)/sumDuration.Seconds()),
				percentile(25).Milliseconds(),
				percentile(50).Milliseconds(),
				percentile(75).Milliseconds(),
				percentile(90).Milliseconds(),
				percentile(99).Milliseconds(),
			),
		)
		messages = append(messages, builder.String())
		builder.Reset()
	}

	return messages
}

type cumulativeStorage struct {
	startTime       time.Time
	queryQuantiles  [5]*psqr.Quantile // 25, 50, 75, 90, 99
	querySamples    int64
	upsertQuantiles [5]*psqr.Quantile // 25, 50, 75, 90, 99
	upsertSamples   int64
}

func newCumulativeStorage() *cumulativeStorage {
	return &cumulativeStorage{
		queryQuantiles: [5]*psqr.Quantile{
			psqr.NewQuantile(0.25),
			psqr.NewQuantile(0.50),
			psqr.NewQuantile(0.75),
			psqr.NewQuantile(0.90),
			psqr.NewQuantile(0.99),
		},
		querySamples: 0,
		upsertQuantiles: [5]*psqr.Quantile{
			psqr.NewQuantile(0.25),
			psqr.NewQuantile(0.50),
			psqr.NewQuantile(0.75),
			psqr.NewQuantile(0.90),
			psqr.NewQuantile(0.99),
		},
		upsertSamples: 0,
	}
}

func (cs *cumulativeStorage) foldWindowIntoSelf(cw *currentWindow) {
	for _, queries := range cw.queries {
		for _, query := range queries {
			for _, quantile := range cs.queryQuantiles {
				quantile.Append(float64(query.clientLatency.Milliseconds()))
			}
		}
		cs.querySamples += int64(len(queries))
	}
	for _, upsert := range cw.upserts {
		for _, quantile := range cs.upsertQuantiles {
			quantile.Append(float64(upsert.duration.Milliseconds()))
		}
	}
	cs.upsertSamples += int64(len(cw.upserts))
}

type reporter struct {
	incoming   chan report
	interval   time.Duration
	window     *currentWindow
	cumulative *cumulativeStorage
}

func newReporter(interval time.Duration) *reporter {
	return &reporter{
		incoming:   make(chan report, 100),
		interval:   interval,
		window:     nil, // initialzed at start()
		cumulative: newCumulativeStorage(),
	}
}

func (r *reporter) sendReport(ctx context.Context, report report) {
	select {
	case <-ctx.Done():
		return
	case r.incoming <- report:
	}
}

func (r *reporter) resetCurrentWindow() {
	r.window = &currentWindow{
		startedAt: time.Now(),
		queries:   make(map[queryTemperature][]*queryStats),
		upserts:   make([]*upsertStats, 0),
	}
}

func (r *reporter) start(ctx context.Context) {
	tkr := time.NewTicker(r.interval)
	defer tkr.Stop()
	r.resetCurrentWindow()
	for {
		select {
		case <-ctx.Done():
			close(r.incoming)
			return
		case report := <-r.incoming:
			if r.cumulative.startTime.IsZero() {
				r.cumulative.startTime = time.Now()
			}
			r.window.processIncomingReport(report)
		case <-tkr.C:
			r.cumulative.foldWindowIntoSelf(r.window)
			r.logReport()
			r.resetCurrentWindow()
		}
	}
}

func (r *reporter) logReport() {
	if r.cumulative.querySamples == 0 && r.cumulative.upsertSamples == 0 {
		return
	}

	var builder strings.Builder

	builder.WriteString(fmt.Sprintf("report for last %s:\n", r.interval))
	for _, message := range r.window.constructLogMessages() {
		builder.WriteString("    - ")
		builder.WriteString(message)
		if !strings.HasSuffix(message, "\n") {
			builder.WriteRune('\n')
		}
	}

	builder.WriteString(
		fmt.Sprintf(
			"    - cumulative (since program start; %s):\n",
			time.Since(r.cumulative.startTime).Round(time.Second),
		),
	)
	if r.cumulative.querySamples > 0 {
		builder.WriteString(
			fmt.Sprintf(
				"        - query latency (ms; over %d total queries): p25=%.1f, p50=%.1f, p75=%.1f, p90=%.1f, p99=%.1f\n",
				r.cumulative.querySamples,
				r.cumulative.queryQuantiles[0].Value(),
				r.cumulative.queryQuantiles[1].Value(),
				r.cumulative.queryQuantiles[2].Value(),
				r.cumulative.queryQuantiles[3].Value(),
				r.cumulative.queryQuantiles[4].Value(),
			),
		)
	}
	if r.cumulative.upsertSamples > 0 {
		builder.WriteString(
			fmt.Sprintf(
				"        - upsert latency (ms; over %d total upserts): p25=%.1f, p50=%.1f, p75=%.1f, p90=%.1f, p99=%.1f\n",
				r.cumulative.upsertSamples,
				r.cumulative.upsertQuantiles[0].Value(),
				r.cumulative.upsertQuantiles[1].Value(),
				r.cumulative.upsertQuantiles[2].Value(),
				r.cumulative.upsertQuantiles[3].Value(),
				r.cumulative.upsertQuantiles[4].Value(),
			),
		)
	}

	fmt.Println(builder.String())
}
