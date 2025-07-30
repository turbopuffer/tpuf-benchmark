// Package template parses JSON templates for benchmarking. It allows dynamic generation of documents
// queries via specifying their shape, and can render as many copies as needed.
package template

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"text/template"
)

// A templated JSON object; not yet "rendered" to a request body.
// A caller can call Render() to get an object that can be used in a request body.
type Template struct {
	parsed *template.Template
	tags   map[string]string
}

// Parse a template from a string. The passed `id` must be unique for each template.
func Parse(data Datasource, id, tmpl string) (*Template, error) {
	var tags map[string]string
	for strings.HasPrefix(tmpl, "#") {
		line, remaining, ok := strings.Cut(tmpl, "\n")
		if !ok {
			return nil, fmt.Errorf("invalid template %q: expected newline after comment", id)
		}
		tmpl = remaining
		k, v, ok := strings.Cut(strings.Trim(line, "# "), "=")
		if !ok {
			return nil, fmt.Errorf(
				"invalid template %q: expected key=value in comment line %q",
				id,
				line,
			)
		}
		if tags == nil {
			tags = make(map[string]string)
		}
		tags[k] = v
	}
	t, err := template.New(id).Funcs(funcMapFromDatasource(data)).Parse(tmpl)
	if err != nil {
		return nil, fmt.Errorf("parsing template %q: %w", id, err)
	}
	return &Template{
		parsed: t,
		tags:   tags,
	}, nil
}

func funcMapFromDatasource(ds Datasource) template.FuncMap {
	return template.FuncMap{
		"id": func() uint64 {
			return ds.Id()
		},
		"vector": func(dims int) (JSONArray, error) {
			vec, err := ds.Vector(dims)
			return JSONArray(vec), err
		},
	}
}

// JSONArray is a slice of float32 that can be rendered as a JSON array.
// Otherwise, text/template will render it as [0.1 0.2 03] which is not valid JSON.
type JSONArray []float32

func (ja JSONArray) String() string {
	if len(ja) == 0 {
		return "[]"
	}
	var buf strings.Builder
	buf.WriteByte('[')
	for i, v := range ja {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(fmt.Sprintf("%.6g", v))
	}
	buf.WriteByte(']')
	return buf.String()
}

// Tag returns the value of a tag with the given name, if it exists.
func (t *Template) Tag(name string) (string, bool) {
	if t.tags == nil {
		return "", false
	}
	v, ok := t.tags[name]
	return v, ok
}

// Render renders the template with the given data and unmarshals it into a given type.
// Returns the value (if nil err), and the size of the rendered JSON object.
func Render[T any](t *Template) (T, uint, error) {
	var result T
	rendered, err := RenderJSON(t)
	if err != nil {
		return result, 0, err
	}
	l := uint(len(rendered))
	if err := json.Unmarshal(rendered, &result); err != nil {
		return result, 0, fmt.Errorf("unmarshalling rendered template: %w", err)
	}
	return result, l, nil
}

// RenderJSON renders the template with the given data and returns the JSON bytes.
func RenderJSON(t *Template) ([]byte, error) {
	var buf bytes.Buffer
	if err := t.parsed.Execute(&buf, nil); err != nil {
		return nil, fmt.Errorf("rendering template: %w", err)
	}
	return buf.Bytes(), nil
}
