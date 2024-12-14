package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/schollz/progressbar/v3"
	"github.com/turbopuffer/tpuf-benchmark/turbopuffer"
	"golang.org/x/sync/errgroup"
)

// The setup phase is split into 3 distinct sections:
// - Deleting existing data from namespaces
// - Upserting new data (as fast as possible) to each namespace
// - Waiting for indexing progress on the last (largest) namespace

type benchmarkStepDelete struct {
	namespaces []*namespace
	totalDocs  int64
	took       *time.Duration
}

func (d *benchmarkStepDelete) before() error {
	d.totalDocs = totalDocuments(d.namespaces)
	fmt.Printf("Deleting existing data from %d namespaces\n", len(d.namespaces))
	fmt.Printf("    - Detected %d existing documents\n", d.totalDocs)
	return nil
}

func (d *benchmarkStepDelete) after() error {
	if d.took == nil {
		return errors.New("missing duration, did run() complete?")
	}
	fmt.Printf(
		"Deleted %d documents in %s\n",
		d.totalDocs,
		d.took.Round(time.Second),
	)
	return nil
}

func (d *benchmarkStepDelete) run(ctx context.Context, logger *slog.Logger) error {
	var (
		start = time.Now()
		eg    = new(errgroup.Group)
		bar   = progressbar.Default(totalDocuments(d.namespaces), "deleting existing documents")
	)
	eg.SetLimit(100)
	for _, ns := range d.namespaces {
		eg.Go(func() error {
			before := ns.documents.Load()
			if err := ns.deleteAllDocuments(ctx); err != nil {
				return fmt.Errorf("failed to delete all documents from namespace: %w", err)
			}
			bar.Add(int(before))
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("failed to delete all documents: %w", err)
	}
	d.took = turbopuffer.AsRef(time.Since(start))
	return nil
}

type benchmarkStepUpsert struct {
	namespaces []*namespace
	dataset    []turbopuffer.Document
	sizes      sizeHistogram
	took       *time.Duration
}

func (u *benchmarkStepUpsert) before() error {
	fmt.Printf(
		"Upserting a total of %d documents across %d namespaces, distribution:\n",
		u.sizes.sum(),
		len(u.namespaces),
	)
	fmt.Printf("    - Min: %d\n", u.sizes.min())
	fmt.Printf("    - Max: %d\n", u.sizes.max())
	for _, p := range []float32{10, 25, 50, 75, 99, 99.9} {
		fmt.Printf("    - p%.1f: %d\n", p, u.sizes.percentile(p))
	}
	return nil
}

func (u *benchmarkStepUpsert) after() error {
	if u.took == nil {
		return errors.New("missing duration, did run() complete?")
	}
	fmt.Printf("Upserted %d documents in %s\n", u.sizes.sum(), u.took.Round(time.Second))
	docsPerSec := float32(u.sizes.sum()) / float32(u.took.Seconds())
	mbps := logicalDocumentSize * docsPerSec / 1024 / 1024
	fmt.Printf(
		"   - %.2f upserts per second (%f MiB/s)\n",
		docsPerSec,
		mbps,
	)
	return nil
}

// Upsert is super CPU-bound, i.e. JSON serialization is expensive
// so we limit concurrency based on the number of CPU cores available.
// If num CPU is > num namespaces, we can afford to allocate more than one
// core to a given namespace at a time.
func (u *benchmarkStepUpsert) run(ctx context.Context, logger *slog.Logger) error {
	if max := u.sizes.max(); max > len(u.dataset) {
		return fmt.Errorf("not enough documents in dataset, need at least %d", max)
	}
	var (
		start      = time.Now()
		numTickets = max(runtime.NumCPU()-1, 1)
		ticketsPer = min(max(numTickets/len(u.namespaces), 1), 8)
		tickets    = make(chan struct{}, numTickets)
		total      = u.sizes.sum()
		eg         = new(errgroup.Group)
		bar        = progressbar.Default(total, "upserting documents")
	)
	for i, ns := range u.namespaces {
		var (
			target = u.sizes[i]
			docs   = u.dataset[:target]
		)
		var acquiredTickets int
		for acquiredTickets < ticketsPer {
			tickets <- struct{}{}
			acquiredTickets++
		}
		eg.Go(func() error {
			defer func() {
				for i := 0; i < acquiredTickets; i++ {
					<-tickets
				}
			}()
			numCores := acquiredTickets
			if _, err := ns.upsertDocumentsBatched(ctx, docs, numCores, logger); err != nil {
				return fmt.Errorf("failed to upsert documents to namespace: %w", err)
			}
			bar.Add(target)
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("failed to upsert documents: %w", err)
	}
	u.took = turbopuffer.AsRef(time.Since(start))
	return nil
}

type benchmarkStepWaitForIndexing struct {
	namespaces          []*namespace
	exhaustiveThreshold int64
	checkInterval       time.Duration
}

func (w *benchmarkStepWaitForIndexing) before() error {
	fmt.Printf("Waiting for indexing progress on %d namespace(s)\n", len(w.namespaces))
	fmt.Printf(
		"    - Waiting until queries exhaustively search less than %d documents\n",
		w.exhaustiveThreshold,
	)
	var namespaceNames []string
	for _, ns := range w.namespaces {
		namespaceNames = append(namespaceNames, ns.handle.Name)
	}
	fmt.Printf("    - Namespaces: %s\n", strings.Join(namespaceNames, ", "))
	return nil
}

func (w *benchmarkStepWaitForIndexing) after() error {
	fmt.Printf(
		"Done, queries to namespaces exhaustively search less than %d documents\n",
		w.exhaustiveThreshold,
	)
	return nil
}

func (w *benchmarkStepWaitForIndexing) run(ctx context.Context, logger *slog.Logger) error {
	var (
		wg = new(sync.WaitGroup)
		pg = progressbar.Default(int64(len(w.namespaces)), "waiting for indexing progress")
	)
	for _, ns := range w.namespaces {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer pg.Add(1)
			ns.waitForIndexing(ctx, logger, w.exhaustiveThreshold, w.checkInterval)
		}()
	}
	wg.Wait()
	return nil
}

func totalDocuments(namespaces []*namespace) int64 {
	var total int64
	for _, ns := range namespaces {
		total += ns.documents.Load()
	}
	return total
}
