package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"sync"
	"text/template"

	"github.com/google/uuid"
)

// TemplateExecutor is an executor for templates.
type TemplateExecutor struct {
	lock      sync.Mutex
	nextId    uint64
	vectors   Source[[]float32]
	documents Source[string]
	msmarco   *MSMarcoSource
}

// ParseTemplate parses a template at a given file path.
// The returned template contains a pointer associated with
// the `TemplateExecutor` instance, i.e. to be able to execute functions
// on the template.
func (te *TemplateExecutor) ParseTemplate(
	ctx context.Context,
	name string,
	filePath string,
) (*template.Template, error) {
	contents, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read template file: %w", err)
	}
	tmplStr := cleanTemplate(string(contents))
	parsed, err := template.New(name).
		Funcs(te.funcMap(ctx)).
		Parse(tmplStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse template: %w", err)
	}
	return parsed, nil
}

func (te *TemplateExecutor) funcMap(ctx context.Context) template.FuncMap {
	return template.FuncMap{
		"N": func(n int) []struct{} {
			return make([]struct{}, n)
		},
		"add": func(a, b int) int {
			return a + b
		},
		"sub": func(a, b int) int {
			return a - b
		},
		"id": func() uint64 {
			te.lock.Lock()
			defer te.lock.Unlock()
			curr := te.nextId
			te.nextId++
			return curr
		},
		"vector": func(dims int) (string, error) {
			te.lock.Lock()
			vec, err := te.vectors.Next(ctx)
			if err != nil {
				te.lock.Unlock()
				return "", err
			}
			te.lock.Unlock()
			var builder strings.Builder
			builder.WriteByte('[')
			for i := 0; i < dims; i++ {
				if i > 0 {
					builder.WriteRune(',')
				}
				builder.WriteString(
					strconv.FormatFloat(
						float64(vec[i%len(vec)]), 'f', -1, 32,
					),
				)
			}
			builder.WriteByte(']')
			return builder.String(), nil
		},
		"document": func() (string, error) {
			te.lock.Lock()
			doc, err := te.documents.Next(ctx)
			te.lock.Unlock()
			return doc, err
		},
		"msmarco_document": func() (string, error) {
			te.lock.Lock()
			doc, err := te.msmarco.NextDocument(ctx)
			te.lock.Unlock()
			return doc, err
		},
		"msmarco_query": func() (string, error) {
			te.lock.Lock()
			query, err := te.msmarco.NextQuery(ctx)
			te.lock.Unlock()
			return query, err
		},
		"number_between": func(min, max uint64) uint64 {
			return min + rand.Uint64N(max-min)
		},
		"string_with_cardinality": stringWithCardinality,
		"string": func(length int) string {
			var builder strings.Builder
			for i := 0; i < length; i++ {
				builder.WriteByte(byte(rand.IntN(26) + 'a'))
			}
			return builder.String()
		},
		"uuid": func() string {
			return uuid.NewString()
		},
	}
}

// stringWithCardinality generates a random string of the given length and cardinality.
// The cardinality is encoded into a string of ascii characters, and then padded
// with zeros to the desired length. Panics if the cardinality overflows the length.
func stringWithCardinality(length, cardinality int) (string, error) {
	const alphabet = "abcdefghijklmnopqrstuvwxyz"
	if cardinality > len(alphabet)*length {
		return "", fmt.Errorf(
			"string cardinality %d is too large for length %d",
			cardinality,
			length,
		)
	}

	var (
		randomValue = rand.IntN(cardinality)
		lastChar    = alphabet[randomValue%len(alphabet)]
		numMaxChars = randomValue / len(alphabet)
	)

	ret := make([]byte, length)
	for i := 0; i < numMaxChars; i++ {
		ret[i] = alphabet[len(alphabet)-1]
	}

	ret[numMaxChars] = lastChar

	for i := numMaxChars + 1; i < length; i++ {
		ret[i] = '0'
	}

	return string(ret), nil
}

// Removes all spaces and newlines from a template file
// If the text is within {{ }}, then it's left as is.
//
// Essentially equivalent to minifying the JSON, before it
// is rendered with actual data.
func cleanTemplate(tmpl string) string {
	var (
		builder  strings.Builder
		inTag    bool
		lastRune rune
	)
	for _, r := range tmpl {
		if (r == '{' || r == '}') && lastRune == r {
			inTag = !inTag
		}
		lastRune = r
		if !inTag && (r == ' ' || r == '\n') {
			continue
		}
		builder.WriteRune(r)
	}
	return builder.String()
}
