package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/turbopuffer/tpuf-benchmark/turbopuffer"

	"github.com/cenkalti/backoff/v4"
)

type namespace struct {
	handle    *turbopuffer.Namespace
	documents atomic.Int64
}

func loadNamespace(
	ctx context.Context,
	client *turbopuffer.Client,
	name string,
) (*namespace, error) {
	handle := client.Namespace(name)

	meta, err := handle.Head(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to perform head request to namespace: %w", err)
	}

	ns := &namespace{
		handle: handle,
	}
	if meta != nil {
		ns.documents.Store(meta.ApproxNumVectors)
	}

	return ns, nil
}

func (ns *namespace) hasAnyDocuments() bool {
	return ns.documents.Load() > 0
}

func (ns *namespace) deleteAllDocuments(ctx context.Context) error {
	if err := ns.handle.DeleteAll(ctx); err != nil {
		var apiErr *turbopuffer.APIError
		if errors.As(err, &apiErr) && apiErr.Code == 404 {
			ns.documents.Store(0)
			return nil
		}
		return fmt.Errorf("failed to delete all documents: %w", err)
	}
	ns.documents.Store(0)
	return nil
}

const (
	logicalDocumentSize = (4 * datasetVectorDimensionality) + 8
	maxBytesPerRequest  = 64 << 20
	maxDocsPerRequest   = maxBytesPerRequest / logicalDocumentSize
)

type upsertStats struct {
	namespace string
	upserted  int
	duration  time.Duration
}

func (ns *namespace) upsertDocumentsBatched(
	ctx context.Context,
	docs []turbopuffer.Document,
) (*upsertStats, error) {
	var (
		start   = time.Now()
		numDocs = len(docs)
	)
	for len(docs) > 0 {
		l := min(len(docs), maxDocsPerRequest)
		upsert := func() error {
			return ns.handle.Upsert(ctx, turbopuffer.UpsertRequest{
				Upserts:            docs[:l],
				DistanceMetric:     turbopuffer.AsRef("euclidean_squared"),
				DisableCompression: true, // We're CPU bound on the client side
			})
		}
		if err := backoff.Retry(upsert, backoff.NewExponentialBackOff()); err != nil {
			return nil, fmt.Errorf("failed to upsert documents: %w", err)
		}
		ns.documents.Add(int64(l))
		docs = docs[l:]
	}
	return &upsertStats{
		namespace: ns.handle.Name,
		upserted:  numDocs,
		duration:  time.Since(start),
	}, nil
}

type queryTemperature string

const (
	queryTemperatureCold queryTemperature = "cold" // <90% in cache
	queryTemperatureWarm queryTemperature = "warm" // 90%+ in cache
	queryTemperatureHot  queryTemperature = "hot"  // 100% in cache
)

func (qt queryTemperature) valid() bool {
	switch qt {
	case queryTemperatureCold, queryTemperatureWarm, queryTemperatureHot:
		return true
	default:
		return false
	}
}

type queryStats struct {
	clientLatency time.Duration
	serverLatency time.Duration
	temperature   queryTemperature
	numExhaustive int64
	namespaceSize int64
}

func (ns *namespace) queryWithRandomDocumentVector(
	ctx context.Context,
	docs []turbopuffer.Document,
) (*queryStats, error) {
	var (
		vector      = docs[rand.IntN(len(docs))].Vector
		clientStart = time.Now()
	)

	_, timings, err := ns.handle.Query(ctx, turbopuffer.QueryRequest{
		Vector: vector,
		TopK:   turbopuffer.AsRef(10),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query: %w", err)
	} else if timings == nil {
		return nil, errors.New("query response missing timings")
	}

	temp := queryTemperature(*timings.CacheTemperature)
	if !temp.valid() {
		return nil, fmt.Errorf("invalid query temperature: %s", temp)
	}

	var numExhaustive int64
	if timings.ExhaustiveCount != nil {
		numExhaustive = *timings.ExhaustiveCount
	}

	stats := &queryStats{
		clientLatency: time.Since(clientStart),
		serverLatency: time.Millisecond * time.Duration(*timings.ProcessingTimeMs),
		temperature:   temp,
		numExhaustive: numExhaustive,
		namespaceSize: ns.documents.Load(),
	}

	return stats, nil
}

func (ns *namespace) waitForIndexing(
	ctx context.Context,
	logger *slog.Logger,
	exhaustiveThreshold int64,
	interval time.Duration,
) error {
	queryVector := make([]float32, datasetVectorDimensionality)
	for i := range queryVector {
		queryVector[i] = rand.Float32()
	}

	if err := ns.handle.IndexHint(ctx, turbopuffer.IndexHintRequest{
		DistanceMetric: "euclidean_squared",
	}); err != nil {
		return fmt.Errorf("failed to send index hint: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		_, stats, err := ns.handle.Query(ctx, turbopuffer.QueryRequest{
			Vector: turbopuffer.NewVectorF32(queryVector),
			TopK:   turbopuffer.AsRef(1),
		})
		if err != nil {
			logger.Warn(
				"failed to query namespace",
				slog.String("error", err.Error()),
				slog.String("namespace", ns.handle.Name),
			)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(interval):
			}
			continue
		} else if stats == nil {
			logger.Warn(
				"query response missing timings",
				slog.String("namespace", ns.handle.Name),
			)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(interval):
			}
			continue
		}

		var exhaustiveCount int64
		if stats.ExhaustiveCount != nil {
			exhaustiveCount = *stats.ExhaustiveCount
		}
		if exhaustiveCount < exhaustiveThreshold {
			return nil
		}

		logger.Debug(
			"namespace is still indexing",
			slog.String("namespace", ns.handle.Name),
			slog.Int64("exhaustive count", exhaustiveCount),
			slog.Int64("threshold", exhaustiveThreshold),
		)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

func (ns *namespace) upsertBatchEvery(
	ctx context.Context,
	logger *slog.Logger,
	dataset []turbopuffer.Document,
	frequency time.Duration,
	batchSize int,
	reports *reporter,
) {
	if frequency == 0 {
		return
	}

	// Avoids all namespaces upserting at the same time
	jitter := time.Duration(rand.Int64N(int64(frequency)))
	time.Sleep(jitter)

	tkr := time.NewTicker(frequency)
	defer tkr.Stop()

	upsertBatch := func() (int, error) {
		var (
			size = ns.documents.Load()
			l    = min(batchSize, len(dataset)-int(size))
		)
		if l <= 0 {
			return 0, nil
		}
		docs := dataset[int(size) : int(size)+l]
		stats, err := ns.upsertDocumentsBatched(ctx, docs)
		if err != nil {
			return 0, fmt.Errorf("failed to upsert documents: %w", err)
		} else if reports != nil {
			reports.sendReport(ctx, report{
				upsert: stats,
			})
		}
		return l, nil
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-tkr.C:
			n, err := upsertBatch()
			if err != nil {
				logger.Warn(
					"failed to upsert batch of documents to namespace",
					slog.String("error", err.Error()),
					slog.String("namespace", ns.handle.Name),
					slog.Int("batch size", n),
					slog.Int64("namespace size", ns.documents.Load()),
				)
				continue
			} else if n == 0 {
				logger.Info(
					"namespace has reached maximum size, stopping upserts",
					slog.String("namespace", ns.handle.Name),
					slog.Int64("size", ns.documents.Load()),
				)
				return
			}
			logger.Debug(
				"upsert batch of documents to namespace",
				slog.String("namespace", ns.handle.Name),
				slog.Int("batch size", n),
				slog.Int64("namespace size", ns.documents.Load()),
			)
		}
	}
}
