package template

import (
	"fmt"
	"math/rand/v2"
)

// Datasource is a source of template data, i.e. providing real data for the templates.
type Datasource interface {
	Id() uint64
	Vector(dims int) ([]float32, error)
}

// NewRandomDatasource creates a new random datasource with the given initial ID.
func NewRandomDatasource() *RandomDatasource {
	return &RandomDatasource{
		id:  0,
		rng: rand.New(rand.NewPCG(42, 69)),
	}
}

// RandomDatasource generates random data for templates.
type RandomDatasource struct {
	id  uint64
	rng *rand.Rand
}

func (ds *RandomDatasource) Id() uint64 {
	id := ds.id
	ds.id++
	return id
}

func (ds *RandomDatasource) Vector(dims int) ([]float32, error) {
	if dims <= 0 {
		return nil, fmt.Errorf("invalid dimensions %d, must be positive", dims)
	}
	vec := make([]float32, dims)
	for i := range vec {
		vec[i] = ds.rng.Float32()
	}
	return vec, nil
}
