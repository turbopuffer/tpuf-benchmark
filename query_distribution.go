package main

import (
	"golang.org/x/exp/rand"
	"gonum.org/v1/gonum/stat/distuv"
)

// QueryDistribution is an interface for types that can generate
// an index into an array of namespaces according to some distribution.
//
// Has a single method `NextIndex` that returns a value in the range [0, n).
// To make implementations easier, `n` is internal to the implementation.
type QueryDistribution interface {
	NextIndex() int
}

// UniformQueryDistribution is a QueryDistribution that generates
// indices uniformly at random.
type UniformQueryDistribution struct {
	n int
}

// NewUniformQueryDistribution creates a new UniformQueryDistribution
// that generates indices in the range [0, n).
func NewUniformQueryDistribution(n int) *UniformQueryDistribution {
	return &UniformQueryDistribution{n: n}
}

func (u *UniformQueryDistribution) NextIndex() int {
	return rand.Intn(u.n)
}

// ParetoQueryDistribution is a QueryDistribution that generates
// indices according to a Pareto distribution.
//
// Internally, maintains an Alias Table for sampling.
type ParetoQueryDistribution struct {
	prob  []float64
	alias []int
}

// NewParetoQueryDistribution creates a new ParetoQueryDistribution.
func NewParetoQueryDistribution(n int, alpha float64) *ParetoQueryDistribution {
	pareto := distuv.Pareto{
		Xm:    1,
		Alpha: alpha,
		Src:   rand.NewSource(42),
	}

	// Compute a set of normalized samples from the Pareto distribution
	// which'll determine our probabilities.
	var (
		samples = make([]float64, n)
		sum     float64
	)
	for i := 0; i < n; i++ {
		s := pareto.Rand()
		samples[i] = s
		sum += s
	}
	for i, s := range samples {
		samples[i] = s / sum
	}

	// Scale the probabilities by `n`.
	var (
		scaled = make([]float64, n)
		small  = make([]int, 0, n)
		large  = make([]int, 0, n)
	)
	for i, s := range samples {
		scaled[i] = s * float64(n)
		if scaled[i] < 1 {
			small = append(small, i)
		} else {
			large = append(large, i)
		}
	}

	ret := &ParetoQueryDistribution{
		prob:  make([]float64, n),
		alias: make([]int, n),
	}

	// Process the queues until they're empty.
	for len(small) > 0 && len(large) > 0 {
		var s, l int
		s, small = small[len(small)-1], small[:len(small)-1]
		l, large = large[len(large)-1], large[:len(large)-1]

		ret.prob[s] = scaled[s]
		ret.alias[s] = l

		scaled[l] = (scaled[l] + scaled[s]) - 1
		if scaled[l] < 1 {
			small = append(small, l)
		} else {
			large = append(large, l)
		}
	}

	// Handle the remaining items
	for len(large) > 0 {
		var g int
		g, large = large[len(large)-1], large[:len(large)-1]
		ret.prob[g] = 1
	}
	for len(small) > 0 {
		var l int
		l, small = small[len(small)-1], small[:len(small)-1]
		ret.prob[l] = 1
	}

	return ret
}

func (p *ParetoQueryDistribution) NextIndex() int {
	var (
		u = rand.Float64()
		i = rand.Intn(len(p.prob))
	)
	if u <= p.prob[i] {
		return i
	}
	return p.alias[i]
}
