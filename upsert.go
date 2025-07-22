package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"text/template"

	"github.com/schollz/progressbar/v3"
	"golang.org/x/sync/errgroup"
)

// This file is used to pre-populate turbopuffer namespaces with documents
// as fast as possible using the official SDK.

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

	eg := new(errgroup.Group)
	eg.SetLimit(64)

	for {
		var err error
		pending, err = makeProgressOn(ctx, docTmpl, upsertTmpl, pending, pb, eg)
		if err != nil {
			return fmt.Errorf("failed to make progress: %w", err)
		} else if len(pending) == 0 {
			break
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
		batch   = min(largest, 32_000)
	)
	if batch == 0 {
		return nil, errors.New("batch size is zero")
	}

	for i := 0; i < len(upserts); i++ {
		var (
			pending = upserts[i].Pending
			take    = min(pending, batch)
		)

		// Generate documents using template
		var docsBuf bytes.Buffer
		for j := 0; j < take; j++ {
			if err := docTmpl.Execute(&docsBuf, nil); err != nil {
				return nil, fmt.Errorf("exec doc template: %w", err)
			}
			if j != take-1 {
				docsBuf.WriteByte(',')
			}
		}

		namespace := upserts[i].Namespace
		docBytes := docsBuf.Bytes()
		eg.Go(func() error {
			if _, _, err := namespace.UpsertPrerendered(ctx, [][]byte{before, docBytes, after}); err != nil {
				return fmt.Errorf("failed to upsert documents: %w", err)
			}
			bar.Add(take)
			return nil
		})

		upserts[i].Pending -= take
	}

	for len(upserts) > 0 && upserts[0].Pending == 0 {
		upserts = upserts[1:]
	}

	return upserts, nil
}
