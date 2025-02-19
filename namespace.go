package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/cenkalti/backoff/v4"
)

// Namespace is a handle to a turbopuffer namespace, and is used
// to perform requests against the namespace.
type Namespace struct {
	name   string
	client *http.Client

	// Templates for upsert and query requests
	queryTmpl  *template.Template
	docTmpl    *template.Template
	upsertTmpl *template.Template
}

// NewNamespace creates a new namespace handle with a given name.
// The namespace will use the provided HTTP client to make requests.
func NewNamespace(
	ctx context.Context,
	client *http.Client,
	name string,
	queryTmpl, docTmpl, upsertTmpl *template.Template,
) *Namespace {
	return &Namespace{
		name:       name,
		client:     client,
		queryTmpl:  queryTmpl,
		docTmpl:    docTmpl,
		upsertTmpl: upsertTmpl,
	}
}

// Clear deletes all documents from the namespace, effectively resetting
// it to an empty state. This will delete all documents in the namespace.
func (n *Namespace) Clear(ctx context.Context) error {
	response, err := n.request(
		ctx,
		http.MethodDelete,
		"/v1/namespaces/"+n.name,
		nil,
	)
	if err != nil {
		if response.Status == http.StatusNotFound {
			return nil // Namespace doesn't exist, nothing to clear
		}
		return fmt.Errorf("failed to clear namespace: %w", err)
	}
	return nil
}

// CurrentSize queries the namespace for its current size, i.e. the
// number of documents it contains.
func (n *Namespace) CurrentSize(ctx context.Context) (int64, error) {
	response, err := n.request(
		ctx,
		http.MethodHead,
		"/v1/namespaces/"+n.name,
		nil,
	)
	if err != nil {
		if response.Status == http.StatusNotFound {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to get namespace size: %w", err)
	}

	if response.Status == http.StatusNotFound {
		return 0, nil
	}

	var numDocuments int64
	if headerValue := response.Headers.Get("X-turbopuffer-Approx-Num-Vectors"); headerValue != "" {
		numDocuments, err = strconv.ParseInt(headerValue, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse namespace size: %w", err)
		}
	} else {
		return 0, fmt.Errorf("missing X-turbopuffer-Approx-Num-Vectors header")
	}

	return numDocuments, nil
}

// PurgeCache purges the cache for the namespace, i.e. it ensures that
// the namespace is in a cold state when the benchmark begins.
func (n *Namespace) PurgeCache(ctx context.Context) error {
	response, err := n.request(
		ctx,
		http.MethodGet,
		fmt.Sprintf("/v1/namespaces/%s/_debug/purge_cache", n.name),
		nil,
	)
	if err != nil {
		if response != nil && response.Status == http.StatusNotFound {
			return nil
		}
		return fmt.Errorf("failed to purge cache: %w", err)
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

	upsertFn := func() error {
		if resp, err := n.request(
			ctx,
			http.MethodPost,
			"/v1/namespaces/"+n.name,
			upsertChunks,
		); err != nil {
			if resp != nil && resp.Status == http.StatusTooManyRequests {
				return err // Retryable
			}
			return backoff.Permanent(fmt.Errorf("failed to upsert documents: %w", err))
		}
		return nil
	}

	if err := backoff.Retry(upsertFn, backoff.WithContext(backoff.NewExponentialBackOff(), ctx)); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return time.Since(start), totalByteSize, nil
		}
		return 0, 0, fmt.Errorf("failed to upsert documents: %w", err)
	}

	return time.Since(start), totalByteSize, nil
}

// ServerTiming is a struct that holds timing information sent back by the server
// after a query. We use this to report query performance.
type ServerTiming struct {
	CacheHitRatio    *float64
	CacheTemperature *CacheTemperature
	ProcessingTimeMs *uint64
	ExhaustiveCount  *int64
}

// Query queries the namespace for a given query, generating the query
// using its query template. Returns server timing information as well as
// client time duration if successful, i.e. we don't care about the actual
// query result.
func (n *Namespace) Query(ctx context.Context) (*ServerTiming, time.Duration, error) {
	var buf bytes.Buffer
	if err := n.queryTmpl.Execute(&buf, nil); err != nil {
		return nil, 0, fmt.Errorf("failed to execute query template: %w", err)
	}

	start := time.Now()

	response, err := n.request(
		ctx,
		http.MethodPost,
		"/v1/namespaces/"+n.name+"/query",
		[][]byte{buf.Bytes()},
	)
	if err != nil {
		if response != nil && response.Status == http.StatusNotFound {
			return nil, 0, nil
		}
		return nil, 0, fmt.Errorf("failed to query namespace: %w", err)
	}

	elapsed := time.Since(start)

	var serverTiming *ServerTiming
	if timing := response.Headers.Get("Server-Timing"); timing != "" {
		serverTiming, err = parseServerTiming(timing)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to parse server timing: %w", err)
		}
	}

	return serverTiming, elapsed, nil
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

func parseServerTiming(timing string) (*ServerTiming, error) {
	var ret ServerTiming
	for _, section := range strings.Split(timing, ", ") {
		keys := strings.Split(section, ";")
		if len(keys) == 0 {
			return nil, fmt.Errorf("invalid server timing section: %s", section)
		}
		key := keys[0]
		for _, part := range keys[1:] {
			part, value, ok := strings.Cut(part, "=")
			if !ok {
				return nil, fmt.Errorf("invalid server timing part: %s", part)
			}
			switch key + "." + part {
			case "cache.hit_ratio":
				parsed, err := strconv.ParseFloat(value, 32)
				if err != nil {
					return nil, fmt.Errorf("failed to parse cache hit ratio: %w", err)
				}
				ret.CacheHitRatio = &parsed
			case "cache.temperature":
				ct := CacheTemperature(value)
				if !ct.Valid() {
					return nil, fmt.Errorf("invalid cache temperature: %s", value)
				}
				ret.CacheTemperature = &ct
			case "processing_time.dur":
				ms, err := strconv.ParseUint(value, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("failed to parse processing time: %w", err)
				}
				ret.ProcessingTimeMs = &ms
			case "exhaustive_search.count":
				count, err := strconv.ParseInt(value, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("failed to parse exhaustive count: %w", err)
				}
				ret.ExhaustiveCount = &count
			}
		}
	}
	return &ret, nil
}

// Helper type to store the result of a request.
type ServerResponse struct {
	Status  int
	Body    []byte
	Headers http.Header
}

func readerOverSlices(slices [][]byte) io.ReadCloser {
	if len(slices) == 0 {
		return nil
	} else if len(slices) == 1 {
		return io.NopCloser(bytes.NewReader(slices[0]))
	}
	return io.NopCloser(&MultiSliceReader{slices: slices})
}

// Does a request to the namespace, using the provided method and path.
// If a body is provided, it's assumed to be JSON and is sent as the
// request body.
func (n *Namespace) request(
	ctx context.Context,
	method, path string,
	slices [][]byte,
) (*ServerResponse, error) {
	endpoint := endpointForPath(path)

	req, err := http.NewRequestWithContext(ctx, method, endpoint, readerOverSlices(slices))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	if *hostHeader != "" {
		req.Host = *hostHeader
	}
	req.Header.Set("Accept-Encoding", "gzip")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", *apiKey))
	req.Header.Set("User-Agent", "tpuf-benchmark")
	if len(slices) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}

	// For gracefully handling redirects requiring a second body
	// read. Not super common, but has come up.
	req.GetBody = func() (io.ReadCloser, error) {
		return readerOverSlices(slices), nil
	}

	resp, err := n.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to do request: %w", err)
	}
	defer resp.Body.Close()

	var respReader io.Reader = resp.Body
	if contentEncoding := resp.Header.Get("Content-Encoding"); contentEncoding != "" {
		switch contentEncoding {
		case "gzip":
			var err error
			respReader, err = gzip.NewReader(resp.Body)
			if err != nil {
				return nil, fmt.Errorf("failed to create gzip reader: %w", err)
			}
		default:
			return nil, fmt.Errorf("unsupported content encoding: %s", contentEncoding)
		}
	}

	respBody, err := io.ReadAll(respReader)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	response := &ServerResponse{
		Status:  resp.StatusCode,
		Body:    respBody,
		Headers: resp.Header,
	}

	if resp.StatusCode >= 400 {
		var apiError struct {
			Error string `json:"error,omitempty"`
		}
		if err := json.Unmarshal(respBody, &apiError); err != nil {
			apiError.Error = string(respBody)
		}
		return response, fmt.Errorf(
			"api error (status %d): %s",
			resp.StatusCode,
			apiError.Error,
		)
	}

	return response, nil
}

func endpointForPath(path string) string {
	baseUrl := *endpoint
	if !strings.HasSuffix(baseUrl, "/") {
		baseUrl += "/"
	}
	path = strings.TrimPrefix(path, "/")
	return baseUrl + path
}
