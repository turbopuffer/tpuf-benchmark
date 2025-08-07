package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"runtime"
	"slices"
	"sync"
	"text/template"

	"github.com/schollz/progressbar/v3"
	"github.com/turbopuffer/turbopuffer-go/option"
	"golang.org/x/sync/errgroup"
)

// This file is used to pre-populate turbopuffer namespaces with documents
// as fast as possible. For large benchmarks, i.e. uploading 100M+ documents,
// it's not feasible to generate and upload documents in a naive way.
//
// Specifically, we:
// - Pre-render a file with documents, in batches of 1M (keeping track of offsets)
// - Build requests by slicing the rendered documents file to get certain document
//   ranges, and then constructing a request with those documents.
// - Keep going until we've uploaded all the required documents, may need to
//   pre-render more files.

// NamespacePendingUpserts is a tuple of a namespace and the number of pending
// upserts for that namespace. Used to keep track of write progress.
type NamespacePendingUpserts struct {
	Namespace *Namespace
	Pending   int
}

// UpsertDocumentsToNamespaces upserts documents to the given namespaces,
// as fast as possible. We want to be able to perform large benchmarks
// over 100M+ documents, so this has to be fast.
func UpsertDocumentsToNamespaces(
	ctx context.Context,
	docTmpl *template.Template,
	upsertTmpl *template.Template,
	namespaces []*Namespace,
	sizes []int,
) error {
	if len(namespaces) != len(sizes) {
		return errors.New("namespaces and sizes must be the same length")
	} else if !slices.IsSorted(sizes) {
		return errors.New("sizes must be sorted")
	}
	pending := make([]NamespacePendingUpserts, len(namespaces))
	for i, ns := range namespaces {
		pending[i] = NamespacePendingUpserts{Namespace: ns, Pending: sizes[i]}
	}

	var totalUpserts int64
	for _, size := range sizes {
		totalUpserts += int64(size)
	}

	concurrentRequests := min(max(1, 4*len(namespaces)), 64)
	log.Printf("upserting documents with %d concurrent batches\n", concurrentRequests)
	pb := progressbar.Default(totalUpserts, "upserting documents")

	eg, egCtx := errgroup.WithContext(ctx)

	eg.SetLimit(concurrentRequests)

upsertLoop:
	for {
		var err error
		pending, err = makeProgressOn(egCtx, docTmpl, upsertTmpl, pending, pb, eg)
		if err != nil {
			return fmt.Errorf("failed to make progress: %w", err)
		} else if len(pending) == 0 {
			break
		}
		select {
		// don't wait for `Wait` to return an error
		case <-egCtx.Done():
			break upsertLoop
		default:
		}
	}

	if err := eg.Wait(); err != nil {
		return fmt.Errorf("failed to wait for upserts: %w", err)
	}

	return nil
}

func makeProgressOn(
	ctx context.Context,
	docTmpl *template.Template,
	upsertTmpl *template.Template,
	upserts []NamespacePendingUpserts,
	bar *progressbar.ProgressBar,
	eg *errgroup.Group,
) ([]NamespacePendingUpserts, error) {
	for len(upserts) > 0 && upserts[0].Pending == 0 {
		upserts = upserts[1:]
	}
	if len(upserts) == 0 {
		return nil, nil
	}

	var upsertRequestBuf bytes.Buffer
	if err := upsertTmpl.Execute(&upsertRequestBuf, struct {
		UpsertBatchPlaceholder string
	}{
		UpsertBatchPlaceholder: string("__UPSERT_BATCH__"),
	}); err != nil {
		return nil, fmt.Errorf("failed to execute upsert template: %w", err)
	}

	before, after, ok := bytes.Cut(upsertRequestBuf.Bytes(), []byte("__UPSERT_BATCH__"))
	if !ok {
		return nil, errors.New("failed to cut upsert request")
	}

	var (
		largest = upserts[len(upserts)-1].Pending
		batch   = min(largest, 250_000)
	)
	if batch == 0 {
		return nil, errors.New("batch size is zero")
	}

	rendered, err := prerenderBuffer(docTmpl, batch)
	if err != nil {
		return nil, fmt.Errorf("failed to prerender buffer: %w", err)
	}

	if len(rendered.Offsets) != batch {
		return nil, errors.New("prerendered buffer has incorrect number of offsets")
	}

	for i := 0; i < len(upserts); i++ {
		var (
			pending = upserts[i].Pending
			take    = min(pending, batch)
		)
		batches, err := rendered.Documents(take, 224<<20)
		if err != nil {
			return nil, fmt.Errorf("failed to get documents: %w", err)
		}

		for _, docs := range batches {
			namespace := upserts[i].Namespace
			eg.Go(func() error {
				if _, _, err := namespace.UpsertPrerendered(ctx, [][]byte{before, docs.Contents, after}, option.WithMaxRetries(10)); err != nil {
					return fmt.Errorf("failed to upsert documents: %w", err)
				}
				bar.Add(docs.NumDocs)
				return nil
			})
		}

		upserts[i].Pending -= take
	}

	for len(upserts) > 0 && upserts[0].Pending == 0 {
		upserts = upserts[1:]
	}

	return upserts, nil
}

type PrerenderedBuffer struct {
	Buffer  []byte
	Offsets []int
}

type PrerenderedBatch struct {
	Contents []byte
	NumDocs  int
}

func (pb *PrerenderedBuffer) Documents(n int, maxBytesPer int) ([]PrerenderedBatch, error) {
	if n > len(pb.Offsets) {
		return nil, errors.New("n is greater than the number of offsets")
	}

	var (
		batches      []PrerenderedBatch
		batchStart   int
		batchSize    int
		batchNumDocs int
	)
	for i := 0; i < n; i++ {
		offset := pb.Offsets[i]
		if offset-batchStart > maxBytesPer {
			batches = append(batches, PrerenderedBatch{
				Contents: pb.Buffer[batchStart : batchStart+batchSize-1],
				NumDocs:  batchNumDocs,
			})
			batchStart = offset
			batchSize = 0
			batchNumDocs = 0
		}
		batchSize = offset - batchStart
		batchNumDocs++
	}

	if batchSize > 0 {
		batches = append(batches, PrerenderedBatch{
			Contents: pb.Buffer[batchStart : batchStart+batchSize-1],
			NumDocs:  batchNumDocs,
		})
	}

	return batches, nil
}

func prerenderBuffer(tmpl *template.Template, n int) (*PrerenderedBuffer, error) {
	var (
		wg        sync.WaitGroup
		todo      = make(chan struct{})
		documents = make(chan []byte)
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			todo <- struct{}{}
		}
		close(todo)
	}()

	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range todo {
				var buf bytes.Buffer
				if err := tmpl.Execute(&buf, nil); err != nil {
					panic(fmt.Errorf("failed to execute template: %w", err))
				}
				documents <- buf.Bytes()
			}
		}()
	}

	go func() {
		wg.Wait()
		close(documents)
	}()

	var (
		buf     bytes.Buffer
		offsets []int
	)
	for doc := range documents {
		offsets = append(offsets, buf.Len())
		if _, err := buf.Write(doc); err != nil {
			return nil, fmt.Errorf("failed to write to prerender buffer: %w", err)
		}
		buf.WriteByte(',')
	}

	return &PrerenderedBuffer{
		Buffer:  buf.Bytes(),
		Offsets: offsets,
	}, nil
}
