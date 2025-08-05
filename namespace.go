package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"text/template"
	"time"

	"github.com/turbopuffer/turbopuffer-go"
	"github.com/turbopuffer/turbopuffer-go/packages/param"
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
	// Build the purge cache endpoint URL
	url := fmt.Sprintf("%s/v1/namespace/%s/_debug/purge_cache", *endpoint, n.ID())
	
	// Create the GET request
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create purge cache request: %w", err)
	}
	
	// Add authorization header
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", *apiKey))
	
	// Get HTTP client from the turbopuffer client
	httpClient := http.DefaultClient
	
	// Execute the request
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute purge cache request: %w", err)
	}
	defer resp.Body.Close()
	
	// Check response status
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("purge cache failed with status %d", resp.StatusCode)
	}
	
	return nil
}

// WarmCache warms the cache for the namespace, i.e. it ensures that the
// namespace is in a warm state when the benchmark begins.
func (n *Namespace) WarmCache(ctx context.Context) error {
	params := turbopuffer.NamespaceHintCacheWarmParams{}
	_, err := n.inner.HintCacheWarm(ctx, params)
	if err != nil {
		return fmt.Errorf("failed to warm cache: %w", err)
	}
	return nil
}

// Upsert upserts a number of documents into the namespace, generating
// the documents using its document template. The caller must be aware of
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

// UpsertPrerendered is the same as `Upsert()`, but instead of generating
// the documents using the upsert template, it takes the upsert request body
// as a parameter. This is used for pre-rendered documents, i.e. when we
// are upserting a ton of documents and want to avoid burning excessive CPU.
func (n *Namespace) UpsertPrerendered(
	ctx context.Context,
	upsertChunks [][]byte,
) (time.Duration, int, error) {
	start := time.Now()

	// Combine chunks and get total size
	jsonData, totalByteSize := combineChunks(upsertChunks)

	// Parse JSON and convert to SDK parameters
	params, err := parseUpsertJSON(jsonData)
	if err != nil {
		return 0, 0, err
	}

	// Execute the write
	_, err = n.inner.Write(ctx, params)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to upsert documents: %w", err)
	}

	return time.Since(start), totalByteSize, nil
}

// combineChunks combines multiple byte chunks into a single byte slice
func combineChunks(chunks [][]byte) ([]byte, int) {
	if len(chunks) == 1 {
		return chunks[0], len(chunks[0])
	}

	// Calculate total size
	var totalSize int
	for _, chunk := range chunks {
		totalSize += len(chunk)
	}

	// Combine all chunks
	result := make([]byte, totalSize)
	offset := 0
	for _, chunk := range chunks {
		copy(result[offset:], chunk)
		offset += len(chunk)
	}

	return result, totalSize
}

// templateUpsertRequest represents the JSON structure from our upsert templates
type templateUpsertRequest struct {
	Upserts        []json.RawMessage `json:"upserts"`
	DistanceMetric string            `json:"distance_metric,omitempty"`
}

// templateAttributeConfig represents schema configuration in templates
type templateAttributeConfig struct {
	Type           string                        `json:"type,omitempty"`
	Ann            bool                          `json:"ann,omitempty"`
	FullTextSearch *templateFullTextSearchConfig `json:"full_text_search,omitempty"`
}

// templateFullTextSearchConfig represents full-text search config in templates
type templateFullTextSearchConfig struct {
	Language        string `json:"language,omitempty"`
	Stemming        bool   `json:"stemming,omitempty"`
	RemoveStopwords bool   `json:"remove_stopwords,omitempty"`
	CaseSensitive   bool   `json:"case_sensitive,omitempty"`
}

// parseUpsertJSON parses the JSON upsert data and converts it to SDK parameters
func parseUpsertJSON(jsonData []byte) (turbopuffer.NamespaceWriteParams, error) {
	var req templateUpsertRequest
	if err := json.Unmarshal(jsonData, &req); err != nil {
		return turbopuffer.NamespaceWriteParams{}, fmt.Errorf("failed to parse upsert JSON: %w", err)
	}

	params := turbopuffer.NamespaceWriteParams{}

	// Set distance metric
	if req.DistanceMetric != "" {
		params.DistanceMetric = turbopuffer.DistanceMetric(req.DistanceMetric)
	}

	// Convert documents
	if len(req.Upserts) > 0 {
		rows, schema := convertTemplateDocuments(req.Upserts)
		params.UpsertRows = rows
		params.Schema = schema
	}

	return params, nil
}

// convertTemplateDocuments converts template documents to SDK row format
func convertTemplateDocuments(docs []json.RawMessage) ([]turbopuffer.RowParam, map[string]turbopuffer.AttributeSchemaConfigParam) {
	rows := make([]turbopuffer.RowParam, len(docs))
	var schemaMap map[string]turbopuffer.AttributeSchemaConfigParam

	for i, docJSON := range docs {
		// First unmarshal to extract schema if present
		var doc map[string]interface{}
		if err := json.Unmarshal(docJSON, &doc); err != nil {
			continue
		}

		row := turbopuffer.RowParam{}
		for k, v := range doc {
			if k == "schema" {
				// Extract schema from first document only
				if i == 0 && schemaMap == nil {
					// Re-unmarshal with proper types for schema
					var docWithSchema struct {
						Schema map[string]templateAttributeConfig `json:"schema"`
					}
					if err := json.Unmarshal(docJSON, &docWithSchema); err == nil {
						schemaMap = convertTemplateSchema(docWithSchema.Schema)
					}
				}
				continue // Don't include schema in row data
			}
			row[k] = v
		}
		rows[i] = row
	}

	return rows, schemaMap
}

// convertTemplateSchema converts template schema to SDK schema format
func convertTemplateSchema(schema map[string]templateAttributeConfig) map[string]turbopuffer.AttributeSchemaConfigParam {
	if schema == nil {
		return nil
	}

	result := make(map[string]turbopuffer.AttributeSchemaConfigParam)
	for name, config := range schema {
		sdkConfig := turbopuffer.AttributeSchemaConfigParam{}

		if config.Type != "" {
			sdkConfig.Type = param.NewOpt(turbopuffer.AttributeType(config.Type))
		}
		if config.Ann {
			sdkConfig.Ann = param.NewOpt(config.Ann)
		}
		if config.FullTextSearch != nil {
			sdkConfig.FullTextSearch = &turbopuffer.FullTextSearchConfigParam{
				Language:        turbopuffer.Language(config.FullTextSearch.Language),
				Stemming:        param.NewOpt(config.FullTextSearch.Stemming),
				RemoveStopwords: param.NewOpt(config.FullTextSearch.RemoveStopwords),
				CaseSensitive:   param.NewOpt(config.FullTextSearch.CaseSensitive),
			}
		}

		result[name] = sdkConfig
	}

	return result
}

// Query queries the namespace for a given query, generating the query
// using its query template. Returns server timing information as well as
// client time duration if successful, i.e. we don't care about the actual
// query result.
func (n *Namespace) Query(ctx context.Context) (*turbopuffer.QueryPerformance, time.Duration, error) {
	// Generate query from template
	var buf bytes.Buffer
	if err := n.queryTmpl.Execute(&buf, nil); err != nil {
		return nil, 0, fmt.Errorf("failed to execute query template: %w", err)
	}

	start := time.Now()

	// Parse and convert to SDK parameters
	params, err := parseQueryJSON(buf.Bytes())
	if err != nil {
		return nil, 0, err
	}

	// Execute the query
	response, err := n.inner.Query(ctx, params)
	if err != nil {
		var apiErr *turbopuffer.Error
		if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound {
			return nil, 0, nil
		}
		return nil, 0, fmt.Errorf("failed to query namespace: %w", err)
	}

	return &response.Performance, time.Since(start), nil
}

// templateQueryRequest represents the JSON structure from our query templates
type templateQueryRequest struct {
	TopK           int               `json:"top_k,omitempty"`
	RankBy         []json.RawMessage `json:"rank_by,omitempty"`
	DistanceMetric string            `json:"distance_metric,omitempty"`
	Filters        json.RawMessage   `json:"filters,omitempty"`
}

// parseQueryJSON parses JSON query data and converts it to SDK parameters
func parseQueryJSON(jsonData []byte) (turbopuffer.NamespaceQueryParams, error) {
	var req templateQueryRequest
	if err := json.Unmarshal(jsonData, &req); err != nil {
		return turbopuffer.NamespaceQueryParams{}, fmt.Errorf("failed to parse query JSON: %w", err)
	}

	params := turbopuffer.NamespaceQueryParams{}

	// Set top_k
	if req.TopK > 0 {
		params.TopK = param.NewOpt(int64(req.TopK))
	}

	// Parse rank_by array
	if len(req.RankBy) > 0 {
		if rb := parseTemplateRankBy(req.RankBy); rb != nil {
			params.RankBy = rb
		}
	}

	// Set distance metric
	if req.DistanceMetric != "" {
		params.DistanceMetric = turbopuffer.DistanceMetric(req.DistanceMetric)
	}

	// TODO: Parse filters when needed

	return params, nil
}

// parseTemplateRankBy parses the rank_by array from template format
func parseTemplateRankBy(rankBy []json.RawMessage) turbopuffer.RankBy {
	if len(rankBy) < 3 {
		return nil
	}

	// Parse field and method
	var field, method string
	if err := json.Unmarshal(rankBy[0], &field); err != nil {
		return nil
	}
	if err := json.Unmarshal(rankBy[1], &method); err != nil {
		return nil
	}

	switch field {
	case "vector":
		if method == "ANN" {
			var vector []float32
			if err := json.Unmarshal(rankBy[2], &vector); err != nil {
				return nil
			}
			return turbopuffer.NewRankByVector(field, vector)
		}
	case "text":
		if method == "BM25" {
			var query string
			if err := json.Unmarshal(rankBy[2], &query); err != nil {
				return nil
			}
			return turbopuffer.NewRankByTextBM25(field, query)
		}
	}

	return nil
}
