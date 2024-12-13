package turbopuffer

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// Client is a Turbopuffer API client. Configured with an HTTP client, API key
// and a base URL to use for requests.
type Client struct {
	inner      *http.Client
	apiKey     string
	baseUrl    string
	hostHeader string
}

// ClientOptions are used to configure a `Client`.
type ClientOptions func(*Client)

// WithHTTPClient allows the caller to customize the underlying HTTP client
// used for requests to the API.
func WithHTTPClient(client *http.Client) ClientOptions {
	return func(c *Client) {
		c.inner = client
	}
}

// WithBaseURL allows the caller to choose a non-default API url to use
// for requests. By default, requests are sent to `https://api.turbopuffer.com`.
func WithBaseURL(baseUrl string) ClientOptions {
	return func(c *Client) {
		c.baseUrl = baseUrl
	}
}

// WithHostHeader allows the caller to set a custom Host header on requests.
func WithHostHeader(hostHeader string) ClientOptions {
	return func(c *Client) {
		c.hostHeader = hostHeader
	}
}

// NewClient constructs a new `Client` object, allowing the caller to configure the
// client by passing zero or more `ClientOptions` functions.
func NewClient(apiKey string, options ...ClientOptions) *Client {
	client := &Client{
		inner:   http.DefaultClient,
		apiKey:  apiKey,
		baseUrl: "https://api.turbopuffer.com",
	}
	for _, opt := range options {
		opt(client)
	}
	return client
}

type request[T any] struct {
	method      string
	path        string
	query       url.Values
	body        *T
	compress    bool
	maxAttempts *int // if none, defaults to 1 (single attempt)
}

func (r *request[T]) bodyBytes() ([]byte, error) {
	var bodyBytes []byte
	if r.body == nil {
		return bodyBytes, nil
	}

	marshaled, err := json.Marshal(r.body)
	if err != nil {
		return nil, fmt.Errorf("marshal json body: %w", err)
	}
	bodyBytes = marshaled

	if r.compress {
		var (
			rdr, wtr = io.Pipe()
			gz       = gzip.NewWriter(wtr)
			gzErr    error
		)

		go func() {
			if n, err := io.Copy(gz, bytes.NewReader(bodyBytes)); err != nil {
				gzErr = err
				return
			} else if n != int64(len(bodyBytes)) {
				gzErr = fmt.Errorf(
					"failed to gzip entire body, wrote %d bytes out of %d",
					n, len(bodyBytes),
				)
				return
			}
			if err := gz.Close(); err != nil {
				gzErr = err
				return
			}
			if err := wtr.Close(); err != nil {
				gzErr = err
				return
			}
		}()

		compressed, err := io.ReadAll(rdr)
		if err != nil {
			return nil, fmt.Errorf("reading gzip compressed body: %w", err)
		} else if gzErr != nil {
			return nil, fmt.Errorf("gzip compress error: %w", err)
		}

		bodyBytes = compressed
	}

	return bodyBytes, nil
}

type response[T any] struct {
	body    *T
	headers http.Header
}

func doRequest[T, E any](
	ctx context.Context,
	client *Client,
	req request[T],
) (*response[E], error) {
	endpoint := client.baseUrl + req.path

	maxAttempts := 1
	if req.maxAttempts != nil && *req.maxAttempts > 1 {
		maxAttempts = *req.maxAttempts
	}

	reqBody, err := req.bodyBytes()
	if err != nil {
		return nil, fmt.Errorf("constructing request body: %w", err)
	}

	var (
		respBody    []byte
		respHeaders http.Header
		tpufError   *APIError
	)
	for attempt := 0; attempt < maxAttempts; attempt++ {
		var bodyReader io.Reader
		if reqBody != nil {
			bodyReader = bytes.NewReader(reqBody)
		}

		tpufError = nil

		httpReq, err := http.NewRequestWithContext(ctx, req.method, endpoint, bodyReader)
		if err != nil {
			return nil, fmt.Errorf("constructing http request: %w", err)
		}

		// Allow non-default host headers to be set on the client.
		if client.hostHeader != "" {
			httpReq.Host = client.hostHeader
		}

		httpReq.Header.Set("Accept-Encoding", "gzip")
		httpReq.Header.Set("Authorization", fmt.Sprintf("Bearer %s", client.apiKey))
		httpReq.Header.Set("User-Agent", "tpuf-go")
		if req.body != nil {
			httpReq.Header.Set("Content-Type", "application/json")
			if req.compress {
				httpReq.Header.Set("Content-Encoding", "gzip")
			}
		}

		resp, err := client.inner.Do(httpReq)
		if err != nil {
			return nil, fmt.Errorf("sending request: %w", err)
		}
		defer resp.Body.Close()

		respHeaders = resp.Header

		var respBodyReader io.Reader = resp.Body
		if respHeaders.Get("Content-Encoding") == "gzip" {
			respBodyReader, err = gzip.NewReader(respBodyReader)
			if err != nil {
				return nil, fmt.Errorf("creating gzip reader: %w", err)
			}
		}

		respBody, err = io.ReadAll(respBodyReader)
		if err != nil {
			return nil, fmt.Errorf("reading response body: %w", err)
		}

		if resp.StatusCode >= 400 {
			var msg string
			if respHeaders.Get("Content-Type") == "application/json" {
				var apiErr struct {
					Error string `json:"error,omitempty"`
				}
				if err := json.Unmarshal(respBody, &apiErr); err == nil {
					msg = apiErr.Error
				}
			} else {
				msg = string(respBody)
			}
			tpufError = &APIError{
				Code:    resp.StatusCode,
				Message: msg,
			}
		}

		// TODO proper backoff here
		if statusCodeShouldRetry(resp.StatusCode) {
			time.Sleep(
				time.Millisecond * 150 * time.Duration(attempt+1),
			) // 150ms, 300ms, 450ms, ...
			continue
		}

		break
	}

	if tpufError != nil {
		return nil, tpufError
	}

	var body *E
	if req.method != http.MethodHead {
		body = new(E)
		if err := json.Unmarshal(respBody, body); err != nil {
			return nil, fmt.Errorf("unmarshalling response body: %w", err)
		}
	}

	return &response[E]{
		body:    body,
		headers: respHeaders,
	}, nil
}

func statusCodeShouldRetry(code int) bool {
	return code >= 500
}

// APIError is an inspectable error type for returning API errors.
type APIError struct {
	Code    int
	Message string
}

func (e APIError) Error() string {
	return fmt.Sprintf("turbopuffer error (http code %d): %s", e.Code, e.Message)
}
