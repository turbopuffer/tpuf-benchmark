package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"text/template"

	"github.com/schollz/progressbar/v3"
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

	pb := progressbar.Default(totalUpserts, "upserting documents")

	for {
		var err error
		pending, err = makeProgressOn(ctx, docTmpl, upsertTmpl, pending, pb)
		if err != nil {
			return fmt.Errorf("failed to make progress: %w", err)
		} else if len(pending) == 0 {
			break
		}
	}

	return nil
}

func makeProgressOn(
	ctx context.Context,
	docTmpl *template.Template,
	upsertTmpl *template.Template,
	upserts []NamespacePendingUpserts,
	bar *progressbar.ProgressBar,
) ([]NamespacePendingUpserts, error) {
	for len(upserts) > 0 && upserts[0].Pending == 0 {
		upserts = upserts[1:]
	}
	if len(upserts) == 0 {
		return nil, nil
	}

	var (
		largest = upserts[len(upserts)-1].Pending
		batch   = min(largest, 1_000_000)
	)
	rendered, err := prerenderBuffer(docTmpl, batch)
	if err != nil {
		return nil, fmt.Errorf("failed to prerender buffer: %w", err)
	}

	eg := new(errgroup.Group)
	eg.SetLimit(36)

	var i int
	for i < len(upserts) {
		n := 1
		for j := i + 1; j < len(upserts); j++ {
			if upserts[j].Pending == upserts[i].Pending {
				n++
				continue
			}
			break
		}

		var (
			pending = upserts[i].Pending
			take    = min(pending, batch)
			batches = rendered.Documents(take, 224<<20)
		)
		for _, docs := range batches {
			var buf bytes.Buffer
			if err := upsertTmpl.Execute(&buf, struct {
				UpsertBatch string
			}{
				UpsertBatch: string(docs),
			}); err != nil {
				return nil, fmt.Errorf("failed to execute upsert template: %w", err)
			}
			slice := buf.Bytes()
			for j := i; j < i+n; j++ {
				eg.Go(func() error {
					if _, _, err := upserts[j].Namespace.UpsertPrerendered(ctx, slice); err != nil {
						return fmt.Errorf("failed to upsert documents: %w", err)
					}
					bar.Add(take)
					return nil
				})
				upserts[j].Pending -= take
			}
		}

		i += n
	}

	if err := eg.Wait(); err != nil {
		return nil, fmt.Errorf("failed to wait for upserts: %w", err)
	}

	return nil, nil
}

type PrerenderedBuffer struct {
	Buffer  []byte
	Offsets []int
}

func (pb *PrerenderedBuffer) Documents(n int, maxBytesPer int) [][]byte {
	var (
		start = pb.Offsets[0]
		end   int
	)
	if n == len(pb.Offsets) {
		end = len(pb.Buffer)
	} else {
		end = int(pb.Offsets[n])
	}
	if end <= maxBytesPer {
		s := pb.Buffer[:end]
		s = s[:len(s)-1]
		return [][]byte{s}
	}
	var slices [][]byte
	for i := 0; i < n; i++ {
		l := pb.Offsets[i] - start
		if l > maxBytesPer {
			s := pb.Buffer[start:pb.Offsets[i]]
			s = s[:len(s)-1]
			slices = append(slices, s)
			start = pb.Offsets[i]
		}
	}
	if start < end {
		s := pb.Buffer[start:end]
		s = s[:len(s)-1]
		slices = append(slices, s)
	}
	return slices
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
