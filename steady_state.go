package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/turbopuffer/tpuf-benchmark/turbopuffer"
)

// The steady-state portion of the benchmark queries/upserts concurrently
// at a configurable rate. After a period of time, the benchmark will exit
// and print a summary of the results.

type benchmarkStepQuery struct {
	namespaces     []*namespace
	dataset        []turbopuffer.Document
	distributedQps float64
	warmupPeriod   time.Duration
	queryDist      queryDistribution
	activePct      float64
	reportInterval time.Duration
	queryHeadstart time.Duration
}

func (q *benchmarkStepQuery) before() error {
	fmt.Printf("Querying namespaces at a distributed %.1f QPS\n", q.distributedQps)

	active := int(q.activePct * float64(len(q.namespaces)))
	if active < 1 {
		active = 1
	}
	fmt.Printf(
		"    - %% of namespaces active: %.1f%% (%d)\n",
		q.activePct*100,
		active,
	)

	fmt.Printf("    - Queries distributed with %s distribution\n", q.queryDist.name())
	if pareto, ok := q.queryDist.(*paretoDistribution); ok {
		tenPct := max(active/10, 1)
		hits, zeros := pareto.distributionExperiment(active*100, active, tenPct)
		fmt.Printf(
			"    - Top 10%% of active namespaces (%d) handle %.1f QPS\n",
			tenPct,
			q.distributedQps*(float64(hits)/(float64(active)*100)),
		)
		if zeros > 0 {
			fmt.Printf("    - Bottom %d active namespaces get no queries\n", zeros)
		}
	} else {
		fmt.Print("\n")
	}

	if q.warmupPeriod > 0 {
		fmt.Printf("    - QPS ramp-up period of %s\n", q.warmupPeriod)
	}
	fmt.Println("")

	return nil
}

func (q *benchmarkStepQuery) after() error {
	return nil
}

func (q *benchmarkStepQuery) run(ctx context.Context, logger *slog.Logger) error {
	if q.distributedQps == 0 {
		return errors.New("no queries to run")
	}

	// Shuffle the namespaces to avoid having the query distribution tied
	// to the size of the namespaces, this isn't realistic
	rand.Shuffle(len(q.namespaces), func(i, j int) {
		q.namespaces[i], q.namespaces[j] = q.namespaces[j], q.namespaces[i]
	})

	// Only do queries to a subset of the namespaces, i.e. some pct
	activeNamespaces := int(q.activePct * float64(len(q.namespaces)))
	if activeNamespaces < 1 {
		activeNamespaces = 1
	}
	active := q.namespaces[:activeNamespaces]

	if q.reportInterval == 0 {
		q.reportInterval = time.Since(time.Time{}) // No reports
	}

	var (
		start     = time.Now()
		reportTkr = time.NewTicker(min(q.reportInterval, time.Second*30))
		reporter  = &queryPerformanceReporter{}
	)
	defer func() {
		reportTkr.Stop()
		reporter.printFinalReport(time.Since(start))
	}()

	firstQueryTime := make([]atomic.Int64, len(active))

	for {
		var (
			targetQps = q.targetQps(time.Since(start))
			interval  = time.Duration(float64(time.Second) * (1 / targetQps))
		)
		select {
		case <-ctx.Done():
			return nil
		case <-reportTkr.C:
			reporter.printReport(q.reportInterval)
		case <-time.After(interval):
			var (
				sampledIdx      = q.queryDist.sample(len(active))
				targetNamespace = active[sampledIdx]
			)

			// If we haven't queried this namespace yet, give the cache a headstart
			// according to the queryHeadstart duration.
			var queryDelay time.Duration
			if fqt := firstQueryTime[sampledIdx].Load(); fqt != 0 {
				t := time.UnixMicro(fqt)
				if since := time.Since(t); since < q.queryHeadstart {
					queryDelay = q.queryHeadstart - since
				}
			} else {
				queryDelay = q.queryHeadstart
				firstQueryTime[sampledIdx].Store(time.Now().UnixMicro())
				go func() {
					if err := targetNamespace.warmupCache(ctx); err != nil {
						logger.Warn(
							"failed to warmup cache for namespace",
							slog.String("namespace", targetNamespace.handle.Name),
							slog.String("error", err.Error()),
						)
					}
				}()
			}

			go func() {
				if queryDelay > 0 {
					select {
					case <-ctx.Done():
						return
					case <-time.After(queryDelay):
					}
				}
				stats, err := targetNamespace.queryWithRandomDocumentVector(
					ctx,
					q.dataset,
				)
				if err != nil {
					if errors.Is(err, context.Canceled) ||
						errors.Is(err, context.DeadlineExceeded) {
						return
					}
					logger.Warn(
						"failed to query namespace",
						slog.String("namespace", targetNamespace.handle.Name),
						slog.String("error", err.Error()),
					)
				} else {
					reporter.addQuery(stats.temperature, stats.clientLatency)
					logger.Debug(
						"queried namespace",
						slog.String("namespace", targetNamespace.handle.Name),
						slog.Duration("client latency", stats.clientLatency),
						slog.Duration("server latency", stats.serverLatency),
						slog.Int64("exhaustive count", stats.numExhaustive),
						slog.Int64("namespace size", stats.namespaceSize),
						slog.String(queryTemperatureAttrKey, string(stats.temperature)),
					)
				}
			}()
		}
	}
}

func (q *benchmarkStepQuery) targetQps(timeSinceStart time.Duration) float64 {
	current := q.distributedQps
	if since := timeSinceStart; since < q.warmupPeriod {
		current = max(
			q.distributedQps/20,
			q.distributedQps*float64(since)/float64(q.warmupPeriod),
		)
	}
	return current
}

type latencies struct {
	samples     []time.Duration
	numInPeriod int
}

func (ql *latencies) viewInPeriod() *latencies {
	return &latencies{
		samples:     ql.samples[len(ql.samples)-ql.numInPeriod:],
		numInPeriod: ql.numInPeriod,
	}
}

func (ql *latencies) sort() {
	slices.Sort(ql.samples)
}

func (ql *latencies) percentile(p float32) time.Duration {
	idx := int(float32(len(ql.samples)) * p / 100)
	return ql.samples[idx]
}

type queryPerformanceReporter struct {
	lock         sync.Mutex
	temperatures map[queryTemperature]*latencies
}

func (qpr *queryPerformanceReporter) addQuery(
	temperature queryTemperature,
	duration time.Duration,
) {
	qpr.lock.Lock()
	defer qpr.lock.Unlock()
	if qpr.temperatures == nil {
		qpr.temperatures = make(map[queryTemperature]*latencies)
	}
	if _, ok := qpr.temperatures[temperature]; !ok {
		qpr.temperatures[temperature] = &latencies{}
	}
	ql := qpr.temperatures[temperature]
	ql.samples = append(ql.samples, duration)
	ql.numInPeriod++
}

// Assumes lock held.
func (qpr *queryPerformanceReporter) temperatureReport(
	temp queryTemperature,
	ql *latencies,
) string {
	ql.sort()
	var builder strings.Builder
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
			"queries (%d), latencies (ms): p25=%d, p50=%d, p75=%d, p90=%d, p99=%d",
			ql.numInPeriod,
			ql.percentile(25).Milliseconds(),
			ql.percentile(50).Milliseconds(),
			ql.percentile(75).Milliseconds(),
			ql.percentile(90).Milliseconds(),
			ql.percentile(99).Milliseconds(),
		),
	)
	return builder.String()
}

func (qpr *queryPerformanceReporter) printReport(reportInterval time.Duration) {
	qpr.lock.Lock()
	defer qpr.lock.Unlock()

	var (
		builder      strings.Builder
		totalQueries int
	)
	for temp, ql := range qpr.temperatures {
		if ql.numInPeriod == 0 {
			continue
		}
		inPeriod := ql.viewInPeriod()
		builder.WriteString("    - ")
		builder.WriteString(qpr.temperatureReport(temp, inPeriod))
		builder.WriteRune('\n')
		totalQueries += inPeriod.numInPeriod
		ql.numInPeriod = 0
	}

	fmt.Printf("%d queries in the last %s\n", totalQueries, reportInterval)
	fmt.Print(builder.String())
	fmt.Println("")
}

func (qpr *queryPerformanceReporter) printFinalReport(totalElapsed time.Duration) {
	qpr.lock.Lock()
	defer qpr.lock.Unlock()

	if qpr.temperatures == nil {
		return
	}

	var (
		combined []time.Duration
		builder  strings.Builder
	)
	for temp, ql := range qpr.temperatures {
		ql.numInPeriod = len(ql.samples)
		combined = append(combined, ql.samples...)
		builder.WriteString("    - ")
		builder.WriteString(qpr.temperatureReport(temp, ql))
		builder.WriteRune('\n')
	}

	slices.Sort(combined)
	percentile := func(p float32) time.Duration {
		idx := int(float32(len(combined)) * p / 100)
		return combined[idx]
	}

	fmt.Println("Final query performance report:")
	fmt.Printf("    - Total elapsed time: %s\n", totalElapsed.Round(time.Second))
	fmt.Printf("    - Total queries: %d\n", len(combined))
	for _, p := range []float32{25, 50, 75, 90, 99} {
		fmt.Printf("    - combined p%.1f: %d ms\n", p, percentile(p).Milliseconds())
	}
	fmt.Print(builder.String())
}

type queryDistribution interface {
	name() string
	sample(max int) int
}

type uniformDistribution struct{}

func (uniformDistribution) name() string {
	return "uniform"
}

func (uniformDistribution) sample(max int) int {
	return rand.IntN(max)
}

type paretoDistribution struct {
	alpha float64
	src   rand.Source
}

func (p paretoDistribution) name() string {
	return "pareto"
}

// Returns the number of hits in the first `at` elements of the distribution
// and the number of zeros, i.e. indexes that are not hit.
func (p paretoDistribution) distributionExperiment(n, max int, at int) (int, int) {
	hits := make([]int, max)
	for i := 0; i < n; i++ {
		hits[p.sample(max)]++
	}
	var (
		zeros    int
		beforeAt int
	)
	for i, hit := range hits {
		if i < at {
			beforeAt += hit
		}
		if hit == 0 {
			zeros++
		}
	}
	return beforeAt, zeros
}

func (p paretoDistribution) sample(max int) int {
	var rnd float64
	if p.src == nil {
		rnd = rand.Float64()
	} else {
		rnd = rand.New(p.src).Float64()
	}
	result := int((1 - math.Pow(rnd, 1/p.alpha)) * float64(max))
	return min(result, max-1)
}

type benchmarkStepUpsertAtRate struct {
	namespaces     []*namespace
	dataset        []turbopuffer.Document
	frequency      time.Duration
	batchSize      int
	reportInterval time.Duration
}

func (u *benchmarkStepUpsertAtRate) before() error {
	fmt.Printf(
		"Upserting %d documents to each namespace (%d) every %s\n",
		u.batchSize,
		len(u.namespaces),
		u.frequency,
	)

	equivWps := float64(u.batchSize) / u.frequency.Seconds()
	fmt.Printf("    - Equivalent WPS to each namespace: %.1f/s\n", equivWps)

	return nil
}

func (u *benchmarkStepUpsertAtRate) after() error {
	return nil
}

func (u *benchmarkStepUpsertAtRate) run(ctx context.Context, logger *slog.Logger) error {
	var (
		start    = time.Now()
		wg       = new(sync.WaitGroup)
		reporter = &upsertPerformanceReporter{}
	)
	defer func() {
		reporter.printFinalReport(len(u.namespaces), time.Since(start))
	}()

	reporterCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		time.Sleep(u.reportInterval / 2) // Offset from the queries
		for {
			select {
			case <-reporterCtx.Done():
				return
			case <-time.After(u.reportInterval):
				reporter.printReport(len(u.namespaces), u.reportInterval)
			}
		}
	}()

	for _, ns := range u.namespaces {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ns.upsertBatchEvery(
				ctx,
				logger,
				u.dataset,
				u.frequency,
				u.batchSize,
				func(stats *upsertStats) {
					reporter.addUpsert(stats)
				},
			)
		}()
	}
	wg.Wait()
	return nil
}

type upsertPerformanceReporter struct {
	lock        sync.Mutex
	stats       []upsertStats
	numInPeriod int
}

func (upr *upsertPerformanceReporter) addUpsert(stats *upsertStats) {
	upr.lock.Lock()
	defer upr.lock.Unlock()
	upr.stats = append(upr.stats, *stats)
	upr.numInPeriod++
}

func (upr *upsertPerformanceReporter) printReport(numNamespaces int, reportInterval time.Duration) {
	if upr.numInPeriod == 0 {
		return
	}

	upr.lock.Lock()
	defer upr.lock.Unlock()

	var latencies []time.Duration
	var totalSize int64
	for _, stats := range upr.stats[len(upr.stats)-upr.numInPeriod:] {
		latencies = append(latencies, stats.duration)
		totalSize += int64(stats.upserted)
	}
	slices.Sort(latencies)

	percentile := func(p float32) time.Duration {
		idx := int(float32(len(latencies)) * p / 100)
		return latencies[idx]
	}

	fmt.Printf(
		"%d upserts (across %d requests) in the last %s\n",
		totalSize,
		upr.numInPeriod,
		reportInterval,
	)

	wps := float64(totalSize) / reportInterval.Seconds()
	mbps := logicalDocumentSize * wps / 1024 / 1024

	fmt.Printf(
		"    - Combined WPS across %d namespaces: %.1f/s (%.1f MiB/s)\n",
		numNamespaces,
		wps,
		mbps,
	)

	fmt.Printf(
		"    - Latencies (ms): p25=%d, p50=%d, p75=%d, p90=%d, p99=%d\n",
		percentile(25).Milliseconds(),
		percentile(50).Milliseconds(),
		percentile(75).Milliseconds(),
		percentile(90).Milliseconds(),
		percentile(99).Milliseconds(),
	)
	fmt.Println("")

	upr.numInPeriod = 0
}

func (upr *upsertPerformanceReporter) printFinalReport(
	numNamespaces int,
	totalElapsed time.Duration,
) {
	if upr.numInPeriod == 0 {
		return
	}

	upr.lock.Lock()
	defer upr.lock.Unlock()

	var latencies []time.Duration
	var totalSize int64
	for _, stats := range upr.stats {
		latencies = append(latencies, stats.duration)
		totalSize += int64(stats.upserted)
	}
	slices.Sort(latencies)

	percentile := func(p float32) time.Duration {
		idx := int(float32(len(latencies)) * p / 100)
		return latencies[idx]
	}

	fmt.Println("Final upsert performance report:")
	fmt.Printf("    - Num namespaces: %d\n", numNamespaces)
	fmt.Printf("    - Total elapsed time: %s\n", totalElapsed.Round(time.Second))
	fmt.Printf("    - Total upserts: %d\n", totalSize)

	wps := float64(totalSize) / totalElapsed.Seconds()
	mbps := logicalDocumentSize * wps / 1024 / 1024

	fmt.Printf(
		"    - Aggregate WPS: %.1f/s (%.1f MiB/s)\n",
		float64(totalSize)/totalElapsed.Seconds(),
		mbps,
	)
	fmt.Printf(
		"    - Latencies (ms): p25=%d, p50=%d, p75=%d, p90=%d, p99=%d\n",
		percentile(25).Milliseconds(),
		percentile(50).Milliseconds(),
		percentile(75).Milliseconds(),
		percentile(90).Milliseconds(),
		percentile(99).Milliseconds(),
	)
}
