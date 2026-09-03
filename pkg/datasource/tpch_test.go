package datasource

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"text/template"
)

func TestTPCHLineitemSF1(t *testing.T) {
	ctx := context.Background()
	src := TPCHLineitem(ctx, 1)
	funcs, release := src.FuncMap(ctx)
	defer release()
	funcs["json"] = func(s string) string {
		b, _ := json.Marshal(s)
		return string(b)
	}
	lineitemFn := funcs["lineitem"].(func() lineitemRow)

	first := lineitemFn()
	if first.OrderKey != 1 || first.LineNumber != 1 {
		t.Fatalf("unexpected first row keys: orderkey=%d linenumber=%d", first.OrderKey, first.LineNumber)
	}
	if first.PartKey != 155190 || first.SuppKey != 7706 {
		t.Fatalf("unexpected first row part/supp: partkey=%d suppkey=%d", first.PartKey, first.SuppKey)
	}
	if first.Quantity != 17 {
		t.Fatalf("unexpected quantity: %d", first.Quantity)
	}
	if math.Abs(first.ExtendedPrice-21168.23) > 1e-9 {
		t.Fatalf("unexpected extendedprice: %v", first.ExtendedPrice)
	}
	if math.Abs(first.Discount-0.04) > 1e-9 {
		t.Fatalf("unexpected discount: %v", first.Discount)
	}
	if math.Abs(first.Revenue-first.ExtendedPrice*first.Discount) > 1e-9 {
		t.Fatalf("l_revenue mismatch: got %v want %v", first.Revenue, first.ExtendedPrice*first.Discount)
	}
	if first.ShipDate != "1996-03-13T00:00:00Z" {
		t.Fatalf("unexpected shipdate: %q", first.ShipDate)
	}
	if first.ID() != uint64(first.OrderKey)*8+uint64(first.LineNumber) {
		t.Fatalf("unexpected id: %d", first.ID())
	}

	for i := 2; i <= 6; i++ {
		row := lineitemFn()
		if row.OrderKey != 1 || row.LineNumber != int64(i) {
			t.Fatalf("row %d: got orderkey=%d linenumber=%d", i, row.OrderKey, row.LineNumber)
		}
		if math.Abs(row.Revenue-row.ExtendedPrice*row.Discount) > 1e-9 {
			t.Fatalf("row %d: l_revenue mismatch", i)
		}
	}

	tmpl, err := template.New("doc").Funcs(funcs).Parse(
		`{{ $l := lineitem }}{"id":{{ $l.ID }},"l_extendedprice":{{ $l.ExtendedPrice }},"l_discount":{{ $l.Discount }},"l_revenue":{{ $l.Revenue }},"l_shipdate":{{ json $l.ShipDate }},"l_quantity":{{ $l.Quantity }}}`,
	)
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	if err := tmpl.Execute(&b, nil); err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(b.String()), &doc); err != nil {
		t.Fatalf("invalid json %q: %v", b.String(), err)
	}
	if _, ok := doc["l_shipdate"].(string); !ok {
		t.Fatalf("unexpected shipdate in json: %v", doc["l_shipdate"])
	}
	if rev, ok := doc["l_revenue"].(float64); !ok || rev < 0 {
		t.Fatalf("unexpected l_revenue: %v", doc["l_revenue"])
	}
}

func TestTPCHLineitemSF10Kind(t *testing.T) {
	if !DatasourceTPCHLineitemSF10.Valid() {
		t.Fatal("TPCHLineitemSF10 should be a valid datasource kind")
	}
	src := Make(context.Background(), DatasourceTPCHLineitemSF10, Config{})
	if _, ok := src.(*tpchLineitemSource); !ok {
		t.Fatalf("expected *tpchLineitemSource, got %T", src)
	}
	if src.(*tpchLineitemSource).scale != 10 {
		t.Fatalf("expected scale 10, got %d", src.(*tpchLineitemSource).scale)
	}
}
