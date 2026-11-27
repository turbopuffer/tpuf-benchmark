package datasource

import (
	"context"
	"fmt"
	"iter"
	"sync"
	"text/template"

	"github.com/pingcap/go-tpc/tpch/dbgen"
)

// TPCHLineitem returns a datasource that yields TPC-H lineitem rows generated
// with dbgen at the given scale factor. Concurrent template renders are
// serialized through the shared pull iterator.
//
// Note: dbgen keeps process-global RNG state, so only one TPCHLineitem
// iterator should be active in a process at a time.
func TPCHLineitem(_ context.Context, scale int64) Source {
	if scale <= 0 {
		panic(fmt.Errorf("TPC-H scale factor must be positive, got %d", scale))
	}
	return &tpchLineitemSource{scale: scale}
}

type tpchLineitemSource struct {
	scale int64

	once sync.Once
	next func() (lineitemRow, error, bool)
}

var _ Source = (*tpchLineitemSource)(nil)

// lineitemRow is one TPC-H lineitem, plus non-spec computed attributes used to
// express TPC-H aggregations against turbopuffer:
//
//	l_disc_price = l_extendedprice * (1 - l_discount)
//	l_charge     = l_extendedprice * (1 - l_discount) * (1 + l_tax)
//	l_revenue    = l_extendedprice * l_discount
type lineitemRow struct {
	OrderKey      int64
	PartKey       int64
	SuppKey       int64
	LineNumber    int64
	Quantity      int64
	ExtendedPrice float64
	Discount      float64
	Tax           float64
	ReturnFlag    string
	LineStatus    string
	ShipDate      string
	CommitDate    string
	ReceiptDate   string
	ShipInstruct  string
	ShipMode      string
	Comment       string
	DiscPrice     float64
	Charge        float64
	Revenue       float64
}

// ID returns a stable document id derived from (l_orderkey, l_linenumber).
// Line numbers are in [1, 7], so orderkey*8 + linenumber is unique.
func (r lineitemRow) ID() uint64 {
	return uint64(r.OrderKey)*8 + uint64(r.LineNumber)
}

func (s *tpchLineitemSource) FuncMap(ctx context.Context) template.FuncMap {
	s.once.Do(func() {
		s.next = lazyPull2(func() iter.Seq2[lineitemRow, error] {
			return s.iterate(ctx)
		})
	})
	return template.FuncMap{
		"lineitem": func() lineitemRow {
			row, err, ok := s.next()
			if !ok {
				panic("TPC-H lineitem source exhausted")
			} else if err != nil {
				panic(err)
			}
			return row
		},
	}
}

func (s *tpchLineitemSource) iterate(ctx context.Context) iter.Seq2[lineitemRow, error] {
	return func(yield func(lineitemRow, error) bool) {
		dbgen.InitDbGen(s.scale)
		loader := &lineitemYieldLoader{ctx: ctx, yield: yield}
		if err := dbgen.DbGen(
			map[dbgen.Table]dbgen.Loader{dbgen.TLine: loader},
			[]dbgen.Table{dbgen.TLine},
		); err != nil && !loader.stopped && ctx.Err() == nil {
			yield(lineitemRow{}, err)
		}
	}
}

// lineitemYieldLoader adapts dbgen's order-oriented lineitem generation onto
// pull-based iteration. Returning an error from Load stops DbGen early when
// the consumer stops yielding.
type lineitemYieldLoader struct {
	ctx     context.Context
	yield   func(lineitemRow, error) bool
	stopped bool
}

func (l *lineitemYieldLoader) Load(item interface{}) error {
	if err := l.ctx.Err(); err != nil {
		l.stopped = true
		return err
	}
	order := item.(*dbgen.Order)
	for i := range order.Lines {
		if !l.yield(lineitemFromDBGen(&order.Lines[i]), nil) {
			l.stopped = true
			return fmt.Errorf("lineitem consumer stopped")
		}
	}
	return nil
}

func (l *lineitemYieldLoader) Flush() error { return nil }

func lineitemFromDBGen(line *dbgen.LineItem) lineitemRow {
	// dbgen stores money fields in cents (extended price, tax) and discount as
	// an integer percentage point (0-10 => 0.00-0.10).
	extendedPrice := float64(line.EPrice) / 100.0
	discount := float64(line.Discount) / 100.0
	tax := float64(line.Tax) / 100.0
	discPrice := extendedPrice * (1 - discount)
	return lineitemRow{
		OrderKey:      int64(line.OKey),
		PartKey:       int64(line.PartKey),
		SuppKey:       int64(line.SuppKey),
		LineNumber:    int64(line.LCnt),
		Quantity:      int64(line.Quantity),
		ExtendedPrice: extendedPrice,
		Discount:      discount,
		Tax:           tax,
		ReturnFlag:    line.RFlag,
		LineStatus:    line.LStatus,
		ShipDate:      line.SDate + "T00:00:00Z",
		CommitDate:    line.CDate + "T00:00:00Z",
		ReceiptDate:   line.RDate + "T00:00:00Z",
		ShipInstruct:  line.ShipInstruct,
		ShipMode:      line.ShipMode,
		Comment:       line.Comment,
		DiscPrice:     discPrice,
		Charge:        discPrice * (1 + tax),
		Revenue:       extendedPrice * discount,
	}
}
