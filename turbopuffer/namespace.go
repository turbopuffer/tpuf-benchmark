package turbopuffer

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// ListNamespacesRequest is used for `ListNamespace` calls.
type ListNamespacesRequest struct {
	Cursor   *string
	Prefix   *string
	PageSize *int // Defaults to 1000 if not set
}

// ListNamespaces lists all namespaces that the API key has access to.
// Paginated; returns a cursor that can be passed into subsequent calls.
// See: https://turbopuffer.com/docs/namespaces
func (c *Client) ListNamespaces(
	ctx context.Context,
	req ListNamespacesRequest,
) ([]string, *string, error) {
	type (
		reqBody  struct{}
		respBody struct {
			Namespaces []struct {
				ID string `json:"id"`
			} `json:"namespaces"`
			NextCursor *string `json:"next_cursor,omitempty"`
		}
	)

	query := url.Values{}
	if req.Cursor != nil {
		query.Set("cursor", *req.Cursor)
	}
	if req.Prefix != nil {
		query.Set("prefix", *req.Prefix)
	}
	if req.PageSize != nil {
		query.Set("page_size", strconv.Itoa(*req.PageSize))
	}

	resp, err := doRequest[reqBody, respBody](
		ctx,
		c,
		request[reqBody]{
			method: http.MethodGet,
			path:   "/v1/vectors",
			query:  query,
		},
	)
	if err != nil {
		return nil, nil, fmt.Errorf("list namespaces: %w", err)
	}

	namespaces := make([]string, 0, len(resp.body.Namespaces))
	for _, ns := range resp.body.Namespaces {
		namespaces = append(namespaces, ns.ID)
	}

	return namespaces, resp.body.NextCursor, nil
}

// Namespace is a lightweight around a `Client` that allows the caller to
// interact with a specific namespace. Each namespace is extremely lightweight,
// and callers are encouraged to logically shard their workloads into as many
// different namespaces as possible.
type Namespace struct {
	client *Client
	Name   string
}

// Namespace creates a new `Namespace` object from a `Client`.
func (c *Client) Namespace(name string) *Namespace {
	return &Namespace{
		client: c,
		Name:   name,
	}
}

type HeadResponse struct {
	ApproxNumVectors int64
	VectorDimensions int
	IndexVersion     *int
}

func newHeadResponse(headers http.Header) (*HeadResponse, error) {
	approxNumVectors, err := strconv.ParseInt(
		headers.Get("X-turbopuffer-Approx-Num-Vectors"),
		10,
		64,
	)
	if err != nil {
		return nil, fmt.Errorf("parsing X-turbopuffer-Approx-Num-Vectors: %w", err)
	}

	vectorDimensions, err := strconv.Atoi(headers.Get("X-turbopuffer-Dimensions"))
	if err != nil {
		return nil, fmt.Errorf("parsing X-turbopuffer-Dimensions: %w", err)
	}

	var indexVersion *int
	if indexVersionHdr := headers.Get("X-turbopuffer-Index-Version"); indexVersionHdr != "None" {
		indexVersionInt, err := strconv.Atoi(indexVersionHdr)
		if err != nil {
			return nil, fmt.Errorf("parsing X-turbopuffer-Index-Version: %w", err)
		}
		indexVersion = &indexVersionInt
	}

	return &HeadResponse{
		ApproxNumVectors: approxNumVectors,
		VectorDimensions: vectorDimensions,
		IndexVersion:     indexVersion,
	}, nil
}

// Head retrieves metadata about the namespace.
// Will return nil if the namespace does not exist.
func (n *Namespace) Head(ctx context.Context) (*HeadResponse, error) {
	resp, err := doRequest[struct{}, struct{}](
		ctx,
		n.client,
		request[struct{}]{
			method: http.MethodHead,
			path:   n.endpointUrl("/"),
		},
	)
	if err != nil {
		var tpufError *APIError
		if errors.As(err, &tpufError) && tpufError.Code == http.StatusNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("head: %w", err)
	}
	return newHeadResponse(resp.headers)
}

// UpsertRequest is used for `Upsert` calls.
type UpsertRequest struct {
	Upserts        []Document                 `json:"upserts,omitempty"`
	DistanceMetric *string                    `json:"distance_metric,omitempty"`
	Schema         map[string]AttributeSchema `json:"schema,omitempty"`

	// Other options, not part of body.
	DisableCompression bool `json:"-"`
	MaxAttempts        *int `json:"-"`
}

// Upsert documents into the namespace. Supports up to 256MB per request.
// See: https://turbopuffer.com/docs/upsert
func (n *Namespace) Upsert(ctx context.Context, req UpsertRequest) error {
	type respBody struct {
		Status string `json:"status"`
	}
	if req.MaxAttempts == nil {
		req.MaxAttempts = AsRef(3)
	}
	_, err := doRequest[UpsertRequest, respBody](
		ctx,
		n.client,
		request[UpsertRequest]{
			method:      http.MethodPost,
			path:        n.endpointUrl("/"),
			body:        &req,
			compress:    !req.DisableCompression,
			maxAttempts: req.MaxAttempts,
		},
	)
	if err != nil {
		return fmt.Errorf("upsert: %w", err)
	}
	return nil
}

// DeleteRequest is used for `Delete` calls.
type DeleteRequest struct {
	IDs []DocumentID
}

// Delete documents from the namespace.
// See: // See: https://turbopuffer.com/docs/upsert
func (n *Namespace) Delete(ctx context.Context, req DeleteRequest) error {
	type (
		deleteUpsert struct {
			ID     DocumentID `json:"id"`
			Vector *Vector    `json:"vector"` // Purposely no "omitempty", we want nulls
		}
		reqBody struct {
			Upserts []deleteUpsert `json:"upserts"`
		}
		respBody struct {
			Status string `json:"status"`
		}
	)
	deleteUpserts := make([]deleteUpsert, 0, len(req.IDs))
	for _, id := range req.IDs {
		deleteUpserts = append(deleteUpserts, deleteUpsert{ID: id, Vector: nil})
	}
	_, err := doRequest[reqBody, respBody](
		ctx,
		n.client,
		request[reqBody]{
			method:      http.MethodPost,
			path:        n.endpointUrl("/"),
			body:        &reqBody{Upserts: deleteUpserts},
			compress:    true,
			maxAttempts: AsRef(3),
		},
	)
	if err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	return nil
}

// QueryRequest is used for `Query` calls.
type QueryRequest struct {
	Vector            *Vector            `json:"vector,omitempty"`
	RankBy            *RankBy            `json:"rank_by,omitempty"`
	DistanceMetric    *string            `json:"distance_metric,omitempty"`
	TopK              *int               `json:"top_k,omitempty"`              // Defaults to 10
	IncludeVectors    *bool              `json:"include_vectors,omitempty"`    // Defaults to false
	IncludeAttributes *IncludeAttributes `json:"include_attributes,omitempty"` // Defaults to none
	Filters           *Filters           `json:"filters,omitempty"`
}

// QueryResponse is the response from a `Query` call.
type QueryResponse []ScoredDocument

type ServerTiming struct {
	CacheHitRatio    *float32
	CacheTemperature *string
	ProcessingTimeMs *uint64
	ExhaustiveCount  *int64
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
			switch fmt.Sprintf("%s.%s", key, part) {
			case "cache.hit_ratio":
				parsed, err := strconv.ParseFloat(value, 32)
				if err != nil {
					return nil, fmt.Errorf("invalid cache.hit_ratio value '%s': %w", value, err)
				}
				ret.CacheHitRatio = AsRef(float32(parsed))
			case "cache.temperature":
				ret.CacheTemperature = AsRef(value)
			case "processing_time.dur":
				ms, err := strconv.ParseUint(value, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid processing_time value '%s': %w", value, err)
				}
				ret.ProcessingTimeMs = AsRef(ms)
			case "exhaustive_search.count":
				count, err := strconv.ParseInt(value, 10, 64)
				if err != nil {
					return nil, fmt.Errorf(
						"invalid exhaustive_search.count value '%s': %w",
						value,
						err,
					)
				}
				ret.ExhaustiveCount = AsRef(count)
			}
		}
	}
	return &ret, nil
}

// Query queries a namespace.
// See: https://turbopuffer.com/docs/query
func (n *Namespace) Query(
	ctx context.Context,
	req QueryRequest,
) (*QueryResponse, *ServerTiming, error) {
	resp, err := doRequest[QueryRequest, QueryResponse](
		ctx,
		n.client,
		request[QueryRequest]{
			method: http.MethodPost,
			path:   n.endpointUrl("/query"),
			body:   &req,
		},
	)
	if err != nil {
		return nil, nil, fmt.Errorf("query: %w", err)
	}

	var serverTiming *ServerTiming
	if timing := resp.headers.Get("Server-Timing"); timing != "" {
		serverTiming, err = parseServerTiming(timing)
		if err != nil {
			return nil, nil, fmt.Errorf("parsing server timing: %w", err)
		}
	}

	return resp.body, serverTiming, nil
}

// ExportRequest is used for `Export` calls.
type ExportRequest struct {
	Cursor *string
}

// Export paginates through all documents in the namespace and returns them.
// See: https://turbopuffer.com/docs/export
func (n *Namespace) Export(ctx context.Context, req ExportRequest) ([]Document, *string, error) {
	type (
		reqBody  struct{}
		respBody struct {
			IDs        []DocumentID                `json:"ids"`
			Vectors    []Vector                    `json:"vectors"`
			Attributes map[string][]AttributeValue `json:"attributes"`
			NextCursor *string                     `json:"next_cursor,omitempty"`
		}
	)

	query := url.Values{}
	if req.Cursor != nil {
		query.Set("cursor", *req.Cursor)
	}

	resp, err := doRequest[reqBody, respBody](
		ctx,
		n.client,
		request[reqBody]{
			method: http.MethodGet,
			path:   n.endpointUrl("/"),
			query:  query,
		},
	)
	if err != nil {
		return nil, nil, fmt.Errorf("export: %w", err)
	}

	numDocs := len(resp.body.IDs)
	if numDocs != len(resp.body.Vectors) {
		return nil, nil, fmt.Errorf("export: mismatched IDs and vectors lengths")
	} else {
		for _, attrs := range resp.body.Attributes {
			if len(attrs) != numDocs {
				return nil, nil, fmt.Errorf("export: mismatched attributes length")
			}
		}
	}

	docs := make([]Document, 0, numDocs)
	for i := 0; i < numDocs; i++ {
		doc := Document{
			ID:         resp.body.IDs[i],
			Vector:     &resp.body.Vectors[i],
			Attributes: make(map[string]AttributeValue),
		}
		for key, values := range resp.body.Attributes {
			doc.Attributes[key] = values[i]
		}
		docs = append(docs, doc)
	}

	return docs, resp.body.NextCursor, nil
}

// DeleteAll deletes an entire namespace, fully.
// See: https://turbopuffer.com/docs/reference/delete-namespace
func (n *Namespace) DeleteAll(ctx context.Context) error {
	type (
		reqBody  struct{}
		respBody struct {
			Status string `json:"status"`
		}
	)
	_, err := doRequest[reqBody, respBody](
		ctx,
		n.client,
		request[reqBody]{
			method: http.MethodDelete,
			path:   n.endpointUrl("/"),
		},
	)
	if err != nil {
		return fmt.Errorf("delete all: %w", err)
	}
	return nil
}

// RecallRequest is used for `Recall` calls.
type RecallRequest struct {
	Number  *int     `json:"num,omitempty"`     // Defaults to 25
	TopK    *int     `json:"top_k,omitempty"`   // Defaults to 10
	Queries *Vector  `json:"queries,omitempty"` // If omitted, samples from the namespace
	Filters *Filters `json:"filters,omitempty"`
}

// RecallResponse is the response from a `Recall` call.
type RecallResponse struct {
	AvgRecall          float32 `json:"avg_recall"`           // Average recall across all queries
	AvgExhaustiveCount float32 `json:"avg_exhaustive_count"` // Avg number of results returned from exhaustive search
	AvgAnnCount        float32 `json:"avg_ann_count"`        // Avg number of results returned from ANN search
}

// Recall measures the recall performance of the namespace.
// See: https://turbopuffer.com/docs/recall
func (n *Namespace) Recall(ctx context.Context, req RecallRequest) (*RecallResponse, error) {
	resp, err := doRequest[RecallRequest, RecallResponse](
		ctx,
		n.client,
		request[RecallRequest]{
			method: http.MethodPost,
			path:   n.endpointUrl("/_debug/recall"),
			body:   &req,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("recall: %w", err)
	}
	return resp.body, nil
}

type IndexHintRequest struct {
	DistanceMetric string `json:"distance_metric"`
}

// IndexHint is used to hint to turbopuffer that a certain namespace should be re-indexed.
// Not guarenteed to actually trigger a re-index.
func (n *Namespace) IndexHint(ctx context.Context, req IndexHintRequest) error {
	if req.DistanceMetric == "" {
		return fmt.Errorf("distance metric must be set")
	}
	_, err := doRequest[IndexHintRequest, struct{}](
		ctx,
		n.client,
		request[IndexHintRequest]{
			method: http.MethodPost,
			path:   n.endpointUrl("/index"),
			body:   &req,
		},
	)
	if err != nil {
		return fmt.Errorf("index hint: %w", err)
	}
	return nil
}

// WarmupCache is a signal to turbopuffer to pre-warm the cache for a namespace.
// This is typically done when we expect a user to start querying a namespace soon.
func (n *Namespace) WarmupCache(ctx context.Context) error {
	_, err := doRequest[struct{}, struct{}](
		ctx,
		n.client,
		request[struct{}]{
			method: http.MethodGet,
			path:   n.endpointUrl("/_debug/warm_cache"),
		},
	)
	if err != nil {
		return fmt.Errorf("warmup cache: %w", err)
	}
	return nil
}

func (n *Namespace) endpointUrl(path string) string {
	return fmt.Sprintf("/v1/vectors/%s%s", n.Name, path)
}
