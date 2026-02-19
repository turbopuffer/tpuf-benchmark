package bench

import (
	"context"
	"encoding"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strings"
	"text/template"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/google/uuid"
	"github.com/turbopuffer/tpuf-benchmark/pkg/datasource"
)

// ParseDefinition parses a benchmark definition from a TOML file.
func ParseDefinition(path string) (*Definition, error) {
	var def Definition
	// Default to waiting for indexing.
	def.Setup.WaitForIndexing = true
	if _, err := toml.DecodeFile(path, &def); err != nil {
		return nil, err
	}
	def.ensureDefaults()
	return &def, nil
}

// Definition defines a benchmark, including the workload and configuration.
type Definition struct {
	Name        string          `toml:"name"`
	Duration    time.Duration   `toml:"duration,omitempty"`
	Namespaces  int             `toml:"namespaces"`
	Description string          `toml:"description,omitempty"`
	Setup       SetupDefinition `toml:"setup"`
	Workloads   Workloads       `toml:"workload"`
}

func (d *Definition) ensureDefaults() {
	d.Namespaces = defaultValue(d.Namespaces, 1)
	d.Duration = defaultValue(d.Duration, time.Minute*10)
	d.Setup.ensureDefaults()
	for _, w := range d.Workloads.Query {
		w.ensureDefaults()
	}
	for _, w := range d.Workloads.Upsert {
		w.ensureDefaults()
	}
}

// Workloads matches TOML [workload.query.<name>] nested tables.
type Workloads struct {
	Query  map[string]*QueryWorkload  `toml:"query"`
	Upsert map[string]*UpsertWorkload `toml:"upsert"`
}

// SetupDefinition defines the setup for the benchmark.
type SetupDefinition struct {
	// NamespaceKey is incorporated into the namespace name, prefixed by
	// whatever prefix the user sets at execution time. The namespace key may be
	// shared across multiple definitions if the data within the namespace is
	// the same.
	//
	// For example, a "vectors_10m" namespace key may be shared by benchmark
	// that tests with a warm cache and one that tests with a cold, purged
	// cache.
	NamespaceKey string `toml:"namespace_key"`
	// Datasource is the datasource to use for the benchmark. It determines
	// which functions are available to the templates and where the source data
	// is pulled from.
	Datasource       datasource.Kind `toml:"datasource"`
	DocumentCount    int             `toml:"document_count"`
	DocumentTemplate Template        `toml:"document_template"`
	UpsertTemplate   Template        `toml:"upsert_template"`
	WaitForIndexing  bool            `toml:"wait_for_indexing,omitempty"`
	// NamespaceSizeDistribution is the distribution of document counts across
	// the namespaces. Options: "uniform", "lognormal".
	NamespaceSizeDistribution string `toml:"namespace_size_distribution,omitempty"`
	// NamespaceSizeLogNormalMu is the mu parameter for the lognormal distribution
	// of namespace sizes.
	NamespaceSizeLogNormalMu float64 `toml:"namespace_size_lognormal_mu,omitempty"`
	// NamespaceSizeLogNormalSigma is the sigma parameter for the lognormal
	// distribution of namespace sizes.
	NamespaceSizeLogNormalSigma float64 `toml:"namespace_size_lognormal_sigma,omitempty"`
}

func (s *SetupDefinition) ensureDefaults() {
	s.Datasource = defaultValue(s.Datasource, datasource.DatasourceCohereWikipediaEmbeddings)
	s.NamespaceSizeDistribution = defaultValue(s.NamespaceSizeDistribution, "uniform")
	s.NamespaceSizeLogNormalMu = defaultValue(s.NamespaceSizeLogNormalMu, 0)
	s.NamespaceSizeLogNormalSigma = defaultValue(s.NamespaceSizeLogNormalSigma, 0.95)
}

// QueryWorkload defines a query workload.
type QueryWorkload struct {
	Datasource  datasource.Kind `toml:"datasource,omitempty"`
	QPS         float64         `toml:"qps"`
	Concurrency int             `toml:"concurrency,omitempty"`
	// ActiveNamespacePct is the percentage of namespaces that will be queried.
	// Defaults to 1.0 (i.e. all namespaces).
	ActiveNamespacePct float64 `toml:"active_namespace_pct,omitempty"`
	// QueryTemplate is the template to use for querying the namespaces.
	QueryTemplate Template `toml:"template"`
	// QueryDistribution is the distribution of queries across the namespaces.
	// Options: "uniform", "round-robin", "pareto".
	QueryDistribution string `toml:"query_distribution,omitempty"`
	// QueryParetoAlpha is the alpha parameter for the Pareto distribution of
	// queries.
	QueryParetoAlpha float64 `toml:"query_pareto_alpha,omitempty"`
}

func (w *QueryWorkload) ensureDefaults() {
	w.Datasource = defaultValue(w.Datasource, datasource.DatasourceRandom)
	w.QPS = defaultValue(w.QPS, 10.0)
	w.Concurrency = defaultValue(w.Concurrency, 8)
	w.ActiveNamespacePct = defaultValue(w.ActiveNamespacePct, 1.0)
	w.QueryDistribution = defaultValue(w.QueryDistribution, "uniform")
	w.QueryParetoAlpha = defaultValue(w.QueryParetoAlpha, 1.5)
}

// UpsertWorkload defines an upsert workload.
type UpsertWorkload struct {
	Datasource       datasource.Kind `toml:"datasource,omitempty"`
	QPS              float64         `toml:"qps"`
	Concurrency      int             `toml:"concurrency,omitempty"`
	UpsertBatchSize  int             `toml:"upsert_batch_size,omitempty"`
	DocumentTemplate Template        `toml:"document_template"`
	UpsertTemplate   Template        `toml:"upsert_template"`
}

func (w *UpsertWorkload) ensureDefaults() {
	w.Datasource = defaultValue(w.Datasource, datasource.DatasourceRandom)
	w.QPS = defaultValue(w.QPS, 10.0)
	w.Concurrency = defaultValue(w.Concurrency, 8)
	w.UpsertBatchSize = defaultValue(w.UpsertBatchSize, 1)
}

func defaultValue[T comparable](v, dv T) T {
	var zero T
	if v == zero {
		return dv
	}
	return v
}

// Template holds a text/template.Template so it can be unmarshaled from TOML
// string values (e.g. inline document_template, query template). The raw text
// is stored during TOML decoding; actual parsing is deferred to Init() when the
// full FuncMap is available.
type Template struct {
	*template.Template
	raw string // cleaned template text, set by UnmarshalText
}

// Assert that *Template implements encoding.TextUnmarshaler.
var _ encoding.TextUnmarshaler = (*Template)(nil)

// UnmarshalText implements encoding.TextUnmarshaler so TOML can decode a
// string into a Template. Parsing is deferred to Init() so that the full
// FuncMap (including datasource functions) is available.
func (t *Template) UnmarshalText(text []byte) error {
	t.raw = cleanTemplate(string(text))
	return nil
}

// NewTemplate creates a Template by parsing the given source with the provided
// FuncMap. This is useful for constructing templates outside of the TOML
// definition flow (e.g. the sanity check).
func NewTemplate(src string, funcs template.FuncMap) (Template, error) {
	parsed, err := template.New("").Funcs(funcs).Parse(src)
	if err != nil {
		return Template{}, err
	}
	return Template{Template: parsed}, nil
}

// parse parses the raw template text with the given FuncMap.
func (t *Template) parse(funcs template.FuncMap) error {
	parsed, err := template.New("").Funcs(funcs).Parse(t.raw)
	if err != nil {
		return err
	}
	t.Template = parsed
	return nil
}

// cleanTemplate removes all spaces and newlines from a template file If the
// text is within {{ }}, then it's left as is. This is essentially equivalent to
// minifying the JSON, before it is rendered with actual data.
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

// Init initializes the benchmark definition's templates and datasources, so
// that the templates are ready to be rendered.
func (d *Definition) Init(ctx context.Context, cfg datasource.Config) error {
	datasources := make(map[datasource.Kind]datasource.Source)
	getDatasource := func(datasourceKind datasource.Kind) datasource.Source {
		ds, ok := datasources[datasourceKind]
		if !ok {
			ds = datasource.Make(ctx, datasourceKind, cfg)
			datasources[datasourceKind] = ds
		}
		return ds
	}
	initTemplates := func(datasourceKind datasource.Kind, tmpls ...*Template) error {
		funcs := makeFuncMap(ctx, getDatasource(datasourceKind))
		for _, tmpl := range tmpls {
			if err := tmpl.parse(funcs); err != nil {
				return err
			}
		}
		return nil
	}

	if err := initTemplates(d.Setup.Datasource,
		&d.Setup.DocumentTemplate,
		&d.Setup.UpsertTemplate); err != nil {
		return fmt.Errorf("parsing setup templates: %w", err)
	}
	for name, workload := range d.Workloads.Query {
		if err := initTemplates(workload.Datasource, &workload.QueryTemplate); err != nil {
			return fmt.Errorf("parsing query workload %q template: %w", name, err)
		}
	}
	for name, workload := range d.Workloads.Upsert {
		if err := initTemplates(workload.Datasource,
			&workload.DocumentTemplate,
			&workload.UpsertTemplate); err != nil {
			return fmt.Errorf("parsing upsert workload %q templates: %w", name, err)
		}
	}
	return nil
}

func makeFuncMap(ctx context.Context, ds datasource.Source) template.FuncMap {
	funcs := template.FuncMap{
		"N": func(n uint64) []struct{} {
			return make([]struct{}, n)
		},
		"add": func(a, b uint64) uint64 {
			return a + b
		},
		"sub": func(a, b uint64) uint64 {
			return a - b
		},
		"id": datasource.MonotonicIDs(),
		"number_between": func(min, max uint64) uint64 {
			return min + rand.Uint64N(max-min)
		},
		"string_with_cardinality": stringWithCardinality,
		"json": func(s string) string {
			b, _ := json.Marshal(s)
			return string(b)
		},
		"string": func(length uint64) string {
			var builder strings.Builder
			builder.Grow(int(length))
			for range length {
				builder.WriteByte(byte(rand.IntN(26) + 'a'))
			}
			return builder.String()
		},
		"uuid": func() string {
			return uuid.NewString()
		},
	}

	for key, fn := range ds.FuncMap(ctx) {
		funcs[key] = fn
	}
	return funcs
}

// stringWithCardinality generates a random string of the given length and
// cardinality. The cardinality is encoded into a string of ascii characters,
// and then padded with zeros to the desired length. Panics if the cardinality
// overflows the length.
func stringWithCardinality(length, cardinality uint64) (string, error) {
	const alphabet = "abcdefghijklmnopqrstuvwxyz"
	if cardinality > uint64(len(alphabet))*length {
		return "", fmt.Errorf(
			"string cardinality %d is too large for length %d",
			cardinality, length)
	}

	var (
		randomValue = rand.IntN(int(cardinality))
		lastChar    = alphabet[randomValue%len(alphabet)]
		numMaxChars = randomValue / len(alphabet)
	)
	ret := make([]byte, length)
	for i := 0; i < numMaxChars; i++ {
		ret[i] = alphabet[len(alphabet)-1]
	}
	ret[numMaxChars] = lastChar
	for i := numMaxChars + 1; i < int(length); i++ {
		ret[i] = '0'
	}
	return string(ret), nil
}
