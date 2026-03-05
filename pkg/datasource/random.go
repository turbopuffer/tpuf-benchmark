package datasource

import (
	"context"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"text/template"
	"time"

	"github.com/cockroachdb/crlib/crrand"
)

func RandomDatasource(ctx context.Context, cfg Config) Source {
	return &randomDatasource{cfg: cfg}
}

type randomDatasource struct {
	cfg Config
}

var _ Source = (*randomDatasource)(nil)

func (d *randomDatasource) FuncMap(ctx context.Context) template.FuncMap {
	ps := &prngState{dims: d.cfg.VectorDimensions}
	ps.ids.perm = crrand.MakePerm64(d.cfg.Seed)
	ps.mu.prng = rand.New(rand.NewPCG(d.cfg.Seed, 0))
	return ps.FuncMap()
}

type prngState struct {
	dims int
	ids  struct {
		perm  crrand.Perm64
		index atomic.Uint64
	}
	mu struct {
		sync.Mutex
		prng *rand.Rand
	}
}

func (s *prngState) nextID() uint64 {
	return s.ids.perm.At(s.ids.index.Add(1))
}

func (s *prngState) nextVector() []float32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	vec := make([]float32, s.dims)
	for i := range vec {
		vec[i] = s.mu.prng.Float32()
	}
	return vec
}

func (s *prngState) FuncMap() template.FuncMap {
	const maxTimestampBeyondEpoch = int64(60 * 365 * 24 * time.Hour / time.Second)
	return template.FuncMap{
		"id": func() uint64 {
			return s.nextID()
		},
		"one_of": func(values ...any) any {
			s.mu.Lock()
			defer s.mu.Unlock()
			return values[s.mu.prng.IntN(len(values))]
		},
		"vector": func(dims int) string {
			return vectorToString(s.nextVector(), dims)
		},
		"timestamp": func() string {
			s.mu.Lock()
			sinceEpoch := s.mu.prng.Int64N(maxTimestampBeyondEpoch)
			s.mu.Unlock()
			return time.Unix(sinceEpoch, 0).Format(time.RFC3339)
		},
	}
}
