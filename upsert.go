package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"text/template"

	"github.com/schollz/progressbar/v3"
	"github.com/turbopuffer/turbopuffer-go"
	"golang.org/x/sync/errgroup"
)

// This file is used to pre-populate turbopuffer namespaces with documents
// as fast as possible. For large benchmarks, i.e. uploading 100M+ documents,
// it's not feasible to generate and upload documents in a naive way.
//
// Specifically, we:
// - Pre-render documents in batches (keeping track of offsets)
// - Build requests by slicing the rendered documents to get certain document
//   ranges, and then constructing a request with those documents.
// - Keep going until we've uploaded all the required documents.
// - Use the official SDK for all API calls.

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
		// Reduced batch size to avoid "entity too large" errors
		batch   = min(largest, 100_000)
	)
	if batch == 0 {
		return nil, errors.New("batch size is zero")
	}

	// Pre-render documents as SDK params once for this batch
	rendered, err := prerenderParams(docTmpl, batch)
	if err != nil {
		return nil, fmt.Errorf("failed to prerender params: %w", err)
	}

	if len(rendered.Rows) != batch {
		return nil, errors.New("prerendered params has incorrect number of rows")
	}

	// Extract distance metric from upsert template
	var distanceMetric string
	var upsertMeta struct {
		DistanceMetric string `json:"distance_metric"`
	}
	if err := json.Unmarshal(append(before, after...), &upsertMeta); err == nil {
		distanceMetric = upsertMeta.DistanceMetric
	}

	// Create work items for all batches across all namespaces
	type WorkItem struct {
		Namespace *Namespace
		Params    turbopuffer.NamespaceWriteParams
		NumDocs   int
	}
	
	// Calculate optimal worker count (CPU cores * 2, capped at 64)
	workerCount := min(max(runtime.GOMAXPROCS(0)*2, 8), 64)
	workChan := make(chan WorkItem, workerCount*4) // Buffer for smoother flow
	
	// Start worker pool
	for w := 0; w < workerCount; w++ {
		eg.Go(func() error {
			for work := range workChan {
				if _, err := work.Namespace.inner.Write(ctx, work.Params); err != nil {
					return fmt.Errorf("failed to write documents: %w", err)
				}
				bar.Add(work.NumDocs)
			}
			return nil
		})
	}
	
	// Distribute work to workers
	go func() {
		defer close(workChan)
		
		for i := 0; i < len(upserts); i++ {
			var (
				pending = upserts[i].Pending
				take    = min(pending, batch)
			)
			
			// Get pre-rendered params for this batch
			// Use smaller batches to avoid entity too large errors
			batches, err := rendered.Batches(take, distanceMetric, 10000)
			if err != nil {
				// Can't return error from goroutine, but this should not fail
				// since we already validated the parameters
				continue
			}

			for _, batchItem := range batches {
				select {
				case workChan <- WorkItem{
					Namespace: upserts[i].Namespace,
					Params:    batchItem.Params,
					NumDocs:   batchItem.NumDocs,
				}:
				case <-ctx.Done():
					return
				}
			}

			upserts[i].Pending -= take
		}
	}()

	for len(upserts) > 0 && upserts[0].Pending == 0 {
		upserts = upserts[1:]
	}

	return upserts, nil
}

// PrerenderedParams holds pre-generated SDK parameters ready for upload
type PrerenderedParams struct {
	Rows   []turbopuffer.RowParam
	Schema map[string]turbopuffer.AttributeSchemaConfigParam
}

// PrerenderedBatch represents a batch of SDK parameters
type PrerenderedBatch struct {
	Params  turbopuffer.NamespaceWriteParams
	NumDocs int
}

// Batches splits the pre-rendered parameters into batches
func (pp *PrerenderedParams) Batches(n int, distanceMetric string, maxDocsPerBatch int) ([]PrerenderedBatch, error) {
	if n > len(pp.Rows) {
		return nil, errors.New("n is greater than the number of rows")
	}

	var batches []PrerenderedBatch
	for i := 0; i < n; i += maxDocsPerBatch {
		end := min(i+maxDocsPerBatch, n)
		batchRows := pp.Rows[i:end]
		
		params := turbopuffer.NamespaceWriteParams{
			UpsertRows: batchRows,
		}
		
		// Set distance metric if specified
		if distanceMetric != "" {
			params.DistanceMetric = turbopuffer.DistanceMetric(distanceMetric)
		}
		
		// Only include schema in first batch
		if i == 0 && pp.Schema != nil {
			params.Schema = pp.Schema
		}
		
		batches = append(batches, PrerenderedBatch{
			Params:  params,
			NumDocs: len(batchRows),
		})
	}

	return batches, nil
}

// prerenderParams pre-renders n documents as SDK parameters in parallel
func prerenderParams(tmpl *template.Template, n int) (*PrerenderedParams, error) {
	var (
		wg        sync.WaitGroup
		todo      = make(chan struct{})
		rows      = make(chan turbopuffer.RowParam)
		mu        sync.Mutex
		parseErr  error
	)

	// Producer: generate work items
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			todo <- struct{}{}
		}
		close(todo)
	}()

	// Workers: render documents and convert to SDK params in parallel
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range todo {
				var buf bytes.Buffer
				if err := tmpl.Execute(&buf, nil); err != nil {
					mu.Lock()
					parseErr = fmt.Errorf("failed to execute template: %w", err)
					mu.Unlock()
					return
				}
				
				// Parse the rendered JSON into a map
				var doc map[string]interface{}
				if err := json.Unmarshal(buf.Bytes(), &doc); err != nil {
					mu.Lock()
					parseErr = fmt.Errorf("failed to unmarshal document: %w", err)
					mu.Unlock()
					return
				}
				
				// Convert to RowParam (excluding schema)
				row := make(turbopuffer.RowParam)
				for k, v := range doc {
					if k != "schema" {
						row[k] = v
					}
				}
				rows <- row
			}
		}()
	}

	// Wait for all workers to finish
	go func() {
		wg.Wait()
		close(rows)
	}()

	// Collect rows
	result := &PrerenderedParams{
		Rows: make([]turbopuffer.RowParam, 0, n),
	}
	
	// Extract schema from first document if present
	firstDoc := true
	for row := range rows {
		if parseErr != nil {
			return nil, parseErr
		}
		result.Rows = append(result.Rows, row)
		
		// Get schema from first document by re-rendering once
		if firstDoc && result.Schema == nil {
			firstDoc = false
			var buf bytes.Buffer
			if err := tmpl.Execute(&buf, nil); err == nil {
				// For now, we'll skip schema extraction in upsert.go
				// The schema should be handled by the namespace setup
			}
		}
	}

	if parseErr != nil {
		return nil, parseErr
	}

	return result, nil
}
