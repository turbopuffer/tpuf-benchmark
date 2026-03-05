package bench

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"text/template"

	"github.com/dustin/go-humanize"
	"github.com/turbopuffer/tpuf-benchmark/pkg/output"
	"github.com/turbopuffer/turbopuffer-go"
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

// setupNamespaces configures all the namespaces we'll be benchmarking with and
// pre-populates them with data according to the provided flags.
//
// Returns the namespaces themselves and their associated sizes.
func setupNamespaces(
	ctx context.Context,
	client *turbopuffer.Client,
	def *Definition,
	cfg RuntimeConfig,
	logger *output.Logger,
) ([]*Namespace, []int, error) {
	if def.Namespaces == 0 {
		return nil, nil, errors.New("namespace count must be greater than 0")
	}

	// Load all the namespace objects
	namespaces := make([]*Namespace, def.Namespaces)
	for i := range def.Namespaces {
		namespaces[i] = NewNamespace(ctx, client,
			fmt.Sprintf("%s_%s_%d", cfg.NamespacePrefix, def.Name, i))
	}

	// Generate sizes for each namespace
	sizes := make([]int, def.Namespaces)
	switch def.Setup.NamespaceSizeDistribution {
	case "uniform":
		logger.Detailf("using uniform size distribution for namespaces")
		eachSize := int64(def.Setup.DocumentCount) / int64(def.Namespaces)
		logger.Detailf("%d documents per namespace", eachSize)
		for i := range sizes {
			sizes[i] = int(eachSize)
		}
	case "lognormal":
		logger.Detailf("using lognormal size distribution for namespaces")
		logger.Detailf("mu: %.3f, sigma: %.3f", def.Setup.NamespaceSizeLogNormalMu, def.Setup.NamespaceSizeLogNormalSigma)
		sizes = generateLognormalSizes(
			def.Namespaces,
			int64(def.Setup.DocumentCount),
			def.Setup.NamespaceSizeLogNormalMu,
			def.Setup.NamespaceSizeLogNormalSigma,
		)
		printSizeDistributionOverview(sizes, logger)
	default:
		return nil, nil, fmt.Errorf(
			"unsupported namespace size distribution: %s",
			def.Setup.NamespaceSizeDistribution,
		)
	}

	// Get the existing sizes of the namespaces.
	// If the namespace doesn't exist, the size will be 0.
	existingSizes, err := forEachNamespace(ctx, namespaces,
		func(ctx context.Context, ns *Namespace) (int, error) {
			size, err := ns.CurrentSize(ctx)
			if err != nil {
				return 0, fmt.Errorf("getting current size: %w", err)
			}
			if size > int64(math.MaxInt) {
				return 0, fmt.Errorf("namespace size too large: %d", size)
			}
			return int(size), nil
		})
	if err != nil {
		return nil, nil, err
	}

	// Check whether any namespaces already have data.
	var totalExisting int64
	for _, s := range existingSizes {
		totalExisting += int64(s)
	}
	if totalExisting > 0 {
		switch cfg.IfNonempty {
		case "clear":
			_, err := forEachNamespace(ctx, namespaces,
				func(ctx context.Context, ns *Namespace) (struct{}, error) {
					return struct{}{}, ns.Clear(ctx)
				})
			if err != nil {
				return nil, nil, fmt.Errorf("clearing namespaces: %w", err)
			}
		case "skip-upsert":
			return namespaces, existingSizes, nil
		case "abort":
			return nil, nil, fmt.Errorf(
				"namespaces are non-empty (%d existing documents); aborting due to --if-nonempty=abort",
				totalExisting,
			)
		default:
			return nil, nil, fmt.Errorf("unsupported --if-nonempty value: %q", cfg.IfNonempty)
		}
	}

	// Upsert documents into the namespaces.
	if err := upsertDocumentsToNamespaces(
		ctx, def.Setup.DocumentTemplate, def.Setup.UpsertTemplate, namespaces, sizes,
		cfg.NamespaceSetupConcurrency, cfg.NamespaceSetupConcurrencyMax,
		logger,
	); err != nil {
		return nil, nil, fmt.Errorf("upserting documents to namespaces: %w", err)
	}

	return namespaces, sizes, nil
}

// namespacePendingUpserts is a tuple of a namespace and the number of pending
// upserts for that namespace. Used to keep track of write progress.
type namespacePendingUpserts struct {
	Namespace *Namespace
	Pending   int
}

// upsertDocumentsToNamespaces upserts documents to the given namespaces, as
// fast as possible.
func upsertDocumentsToNamespaces(
	ctx context.Context,
	docTmpl Template,
	upsertTmpl Template,
	namespaces []*Namespace,
	sizes []int,
	setupConcurrency, setupConcurrencyMax int,
	logger *output.Logger,
) error {
	logger.NextStage(output.StageUpserting)
	logger.Detailf("upserting documents to namespaces: %d namespaces, %d sizes", len(namespaces), len(sizes))
	if len(namespaces) != len(sizes) {
		return errors.New("namespaces and sizes must be the same length")
	} else if !slices.IsSorted(sizes) {
		return errors.New("sizes must be sorted")
	}
	pending := make([]namespacePendingUpserts, len(namespaces))
	for i, ns := range namespaces {
		pending[i] = namespacePendingUpserts{Namespace: ns, Pending: sizes[i]}
	}

	var totalUpserts uint64
	for _, size := range sizes {
		totalUpserts += uint64(size)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	prerenderedBuffers := prerenderTemplateBuffers(ctx, docTmpl.Template, runtime.GOMAXPROCS(0), totalUpserts)
	concurrentRequests := min(max(1, setupConcurrency*len(namespaces)), setupConcurrencyMax)
	task := logger.Task("upserting documents", int(totalUpserts))
	logger.Detailf("upserting documents with %d concurrent batches\n", concurrentRequests)

	var upsertRequestBuf bytes.Buffer
	if err := upsertTmpl.Execute(&upsertRequestBuf, struct {
		UpsertBatchPlaceholder string
	}{
		UpsertBatchPlaceholder: string("__UPSERT_BATCH__"),
	}); err != nil {
		return fmt.Errorf("failed to execute upsert template: %w", err)
	}
	requestPrefix, requestSuffix, ok := bytes.Cut(upsertRequestBuf.Bytes(), []byte("__UPSERT_BATCH__"))
	if !ok {
		return errors.New("failed to cut upsert request")
	}

	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(concurrentRequests)

upsertLoop:
	for {
		var pb prerenderedBuffer
		select {
		case <-egCtx.Done():
			break upsertLoop
		case pb = <-prerenderedBuffers:
		}

		var err error
		pending, err = writeAcrossNamespaces(egCtx, pb, pending, eg, requestPrefix, requestSuffix, task)
		if err != nil {
			return fmt.Errorf("failed to write across namespaces: %w", err)
		} else if len(pending) == 0 {
			break
		}
	}
	return eg.Wait()
}

// prerenderedBuffer is a buffer of prerendered, comma-separated documents.
type prerenderedBuffer struct {
	Buffer []byte
	// Offsets is a list of byte offsets within the buffer where each document
	// begins. There are len(Offsets) documents in the buffer.
	Offsets []int
}

func (pb *prerenderedBuffer) getNDocuments(n int) []byte {
	if n > len(pb.Offsets) {
		panic(errors.New("n is greater than the number of offsets"))
	} else if n == len(pb.Offsets) {
		return pb.Buffer
	}
	// NB: -1 to exclude the trailing comma separator
	return pb.Buffer[:pb.Offsets[n]-1]
}

func writeAcrossNamespaces(
	ctx context.Context,
	pb prerenderedBuffer,
	pending []namespacePendingUpserts,
	eg *errgroup.Group,
	requestPrefix, requestSuffix []byte,
	task *output.TaskProgress,
) ([]namespacePendingUpserts, error) {
	for len(pending) > 0 && pending[0].Pending == 0 {
		pending = pending[1:]
	}
	for i := range pending {
		n := min(pending[i].Pending, len(pb.Offsets))
		ns := pending[i].Namespace
		docs := pb.getNDocuments(n)
		eg.Go(func() error {
			_, _, err := ns.UpsertPrerendered(ctx,
				[][]byte{requestPrefix, docs, requestSuffix},
				option.WithMaxRetries(20))
			if err != nil {
				return fmt.Errorf("failed to upsert documents: %w", err)
			}
			task.Advance(n, "upserted %d (%s) documents", n, humanize.Bytes(uint64(len(docs))))
			return nil
		})
		pending[i].Pending -= n
	}
	for len(pending) > 0 && pending[0].Pending == 0 {
		pending = pending[1:]
	}
	return pending, nil
}

// prerenderTemplateBuffers launches goroutines to prerender the provided
// template into batches of up to 128 MiB of documents. It returns a channel of
// prerendered buffers. The concurrency parameter controls the number of worker
// goroutines to launch.
//
// When [maxDocs] documents have been pre-rendered, rendering stops, the
// goroutines exit, and the channel is closed. If the context is cancelled, the
// rendering stops early.
func prerenderTemplateBuffers(
	ctx context.Context,
	tmpl *template.Template,
	concurrency int,
	maxDocs uint64,
) <-chan prerenderedBuffer {
	const maxBytesPer = 128 << 20 // 128 MiB
	var (
		wg    sync.WaitGroup
		count atomic.Uint64
		ch    = make(chan prerenderedBuffer)
	)
	for range concurrency {
		wg.Go(func() {
			var buf bytes.Buffer
			var offsets []int
			buf.Grow(maxBytesPer)

			flushBuf := func() prerenderedBuffer {
				pb := prerenderedBuffer{
					Buffer:  slices.Clone(buf.Bytes()),
					Offsets: slices.Clone(offsets),
				}
				buf.Reset()
				offsets = offsets[:0]
				return pb
			}

			for count.Add(1) <= maxDocs {
				if buf.Len() > 0 {
					buf.WriteByte(',')
				}
				offsets = append(offsets, buf.Len())
				if err := tmpl.Execute(&buf, nil); err != nil {
					panic(fmt.Errorf("failed to execute template: %w", err))
				}
				if buf.Len() > maxBytesPer {
					select {
					case ch <- flushBuf():
					case <-ctx.Done():
						return
					}
				}
			}
			if buf.Len() > 0 {
				select {
				case ch <- flushBuf():
				case <-ctx.Done():
					return
				}
			}
		})
	}
	go func() {
		wg.Wait()
		close(ch)
	}()
	return ch
}
