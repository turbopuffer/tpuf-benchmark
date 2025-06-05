package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"text/template"
	"time"

	"github.com/turbopuffer/turbopuffer-go"
	"github.com/turbopuffer/turbopuffer-go/packages/respjson"
)

// Namespace is a handle to a turbopuffer namespace, and is used
// to perform requests against the namespace.
type Namespace struct {
	client *turbopuffer.Client
	inner  turbopuffer.Namespace

	// Templates for upsert and query requests
	queryTmpl  *template.Template
	docTmpl    *template.Template
	upsertTmpl *template.Template
}

// NewNamespace creates a new namespace handle with a given name.
// The namespace will use the provided HTTP client to make requests.
func NewNamespace(
	ctx context.Context,
	client *turbopuffer.Client,
	name string,
	queryTmpl, docTmpl, upsertTmpl *template.Template,
) *Namespace {
	return &Namespace{
		client:     client,
		inner:      client.Namespace(name),
		queryTmpl:  queryTmpl,
		docTmpl:    docTmpl,
		upsertTmpl: upsertTmpl,
	}
}

// ID reports the ID of the namespace.
func (n *Namespace) ID() string {
	return n.inner.ID()
}

// Clear deletes all documents from the namespace, effectively resetting
// it to an empty state. This will delete all documents in the namespace.
func (n *Namespace) Clear(ctx context.Context) error {
	_, err := n.inner.DeleteAll(ctx, turbopuffer.NamespaceDeleteAllParams{})
	if err != nil {
		var apiErr *turbopuffer.Error
		if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound {
			return nil // Namespace doesn't exist, nothing to clear
		}
		return fmt.Errorf("failed to clear namespace: %w", err)
	}
	return nil
}

// CurrentSize queries the namespace for its current size, i.e. the
// number of documents it contains.
func (n *Namespace) CurrentSize(ctx context.Context) (int64, error) {
	response, err := n.inner.Query(ctx, turbopuffer.NamespaceQueryParams{
		AggregateBy: map[string]turbopuffer.AggregateBy{
			"count": turbopuffer.NewAggregateByCount("id"),
		},
	})
	if err != nil {
		var apiErr *turbopuffer.Error
		if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to get namespace size: %w", err)
	}
	count := response.Aggregations["count"].(respjson.Number)
	return count.Int64()
}

// PurgeCache purges the cache for the namespace, i.e. it ensures that
// the namespace is in a cold state when the benchmark begins.
func (n *Namespace) PurgeCache(ctx context.Context) error {
	url := fmt.Sprintf("/v1/namespaces/%s/_debug/purge_cache", n.ID())
	err := n.client.Get(ctx, url, nil, nil)
	if err != nil {
		var apiErr *turbopuffer.Error
		if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound {
			return nil
		}
		return fmt.Errorf("failed to purge cache: %w", err)
	}
	return nil
}

// WarmCache warms the cache for the namespace, i.e. it ensures that the
// namespace is in a warm state when the benchmark begins.
func (n *Namespace) WarmCache(ctx context.Context) error {
	url := fmt.Sprintf("/v1/namespaces/%s/_debug/warm_cache", n.ID())
	err := n.client.Get(ctx, url, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to warm cache: %w", err)
	}
	return nil
}

// Upsert upserts a number of documents into the namespace, generating
// the documents using its upsert template. The caller must be aware of
// batch sizes, and tune accordingly (i.e. the API may not accept more
// than a certain number of documents in a single request).
func (n *Namespace) Upsert(ctx context.Context, numDocs int) (time.Duration, int, error) {
	var docsBuf bytes.Buffer
	for i := 0; i < numDocs; i++ {
		if err := n.docTmpl.Execute(&docsBuf, nil); err != nil {
			return 0, 0, fmt.Errorf("exec doc template: %w", err)
		}
		if i != numDocs-1 {
			docsBuf.WriteByte(',')
		}
	}

	var upsertBuf bytes.Buffer
	if err := n.upsertTmpl.Execute(&upsertBuf, struct {
		UpsertBatchPlaceholder string
	}{
		UpsertBatchPlaceholder: "__UPSERT_BATCH__",
	}); err != nil {
		return 0, 0, fmt.Errorf("failed to execute upsert template: %w", err)
	}

	before, after, ok := bytes.Cut(upsertBuf.Bytes(), []byte("__UPSERT_BATCH__"))
	if !ok {
		return 0, 0, fmt.Errorf("failed to cut upsert request")
	}

	return n.UpsertPrerendered(ctx, [][]byte{before, docsBuf.Bytes(), after})
}

type MultiSliceReader struct {
	slices [][]byte
	idx    int
	offset int
}

func (msr *MultiSliceReader) Read(p []byte) (n int, err error) {
	if msr.idx >= len(msr.slices) {
		return 0, io.EOF
	}
	slice := msr.slices[msr.idx]
	remaining := len(slice) - msr.offset

	if remaining <= 0 {
		msr.idx++
		msr.offset = 0
		return msr.Read(p)
	}

	cl := min(len(p), remaining)
	copy(p, slice[msr.offset:msr.offset+cl])
	msr.offset += cl
	return cl, nil
}

func readerOverSlices(slices [][]byte) io.ReadCloser {
	if len(slices) == 0 {
		return nil
	} else if len(slices) == 1 {
		return io.NopCloser(bytes.NewReader(slices[0]))
	}
	return io.NopCloser(&MultiSliceReader{slices: slices})
}

// UpsertPrerendered is the same as `Upsert()`, but instead of generating
// the documents using the upsert template, it takes the upsert request body
// as a parameter. This is used for pre-rendered documents, i.e. when we
// are upserting a ton of documents and want to avoid burning excessive CPU.
func (n *Namespace) UpsertPrerendered(
	ctx context.Context,
	upsertChunks [][]byte,
) (time.Duration, int, error) {
	start := time.Now()

	var totalByteSize int
	for _, chunk := range upsertChunks {
		totalByteSize += len(chunk)
	}

	url := fmt.Sprintf("/v1/namespaces/%s", n.ID())
	if err := n.client.Post(ctx, url, readerOverSlices(upsertChunks), nil); err != nil {
		return 0, 0, fmt.Errorf("failed to upsert documents: %w", err)
	}

	return time.Since(start), totalByteSize, nil
}

// Query queries the namespace for a given query, generating the query
// using its query template. Returns server timing information as well as
// client time duration if successful, i.e. we don't care about the actual
// query result.
func (n *Namespace) Query(ctx context.Context) (*turbopuffer.QueryPerformance, time.Duration, error) {
	var buf bytes.Buffer
	if err := n.queryTmpl.Execute(&buf, nil); err != nil {
		return nil, 0, fmt.Errorf("failed to execute query template: %w", err)
	}

	start := time.Now()

	url := fmt.Sprintf("/v2/namespaces/%s/query", n.ID())
	var response turbopuffer.NamespaceQueryResponse
	if err := n.client.Post(ctx, url, buf.Bytes(), &response); err != nil {
		var apiErr *turbopuffer.Error
		if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound {
			return nil, 0, nil
		}
		return nil, 0, fmt.Errorf("failed to query namespace: %w", err)
	}

	elapsed := time.Since(start)

	return &response.Performance, elapsed, nil
}

// CacheTemperature is an enum over the possible cache temperatures
// reported by the turbopuffer API for a given query.
type CacheTemperature string

// Possible cache temperatures reported by the turbopuffer API.
const (
	CacheTemperatureHot  CacheTemperature = "hot"
	CacheTemperatureWarm CacheTemperature = "warm"
	CacheTemperatureCold CacheTemperature = "cold"
)

// Valid returns true if the provided CacheTemperature is valid
func (ct CacheTemperature) Valid() bool {
	switch ct {
	case CacheTemperatureHot, CacheTemperatureWarm, CacheTemperatureCold:
		return true
	default:
		return false
	}
}

// All returns a slice of all possible CacheTemperature values.
func AllCacheTemperatures() []CacheTemperature {
	return []CacheTemperature{
		CacheTemperatureCold,
		CacheTemperatureWarm,
		CacheTemperatureHot,
	}
}
