package main

import (
	"math"
	"math/rand/v2"
	"slices"
)

type sizeHistogram []int

func (s sizeHistogram) avg() float64 {
	var sum int
	for _, size := range s {
		sum += size
	}
	return float64(sum) / float64(len(s))
}

func (s sizeHistogram) min() int {
	return s[0]
}

func (s sizeHistogram) max() int {
	return s[len(s)-1]
}

func (s sizeHistogram) percentile(p float32) int {
	if p < 0 || p > 100 {
		panic("percentile out of range")
	}
	return s[int(float32(len(s))*p/100)]
}

func (s sizeHistogram) sum() int64 {
	var sum int64
	for _, size := range s {
		sum += int64(size)
	}
	return sum
}

func generateSizesUniform(n, size int) sizeHistogram {
	sizes := make(sizeHistogram, n)
	for i := 0; i < n; i++ {
		sizes[i] = size
	}
	return sizes
}

func generateSizesLognormal(n, min, max int, mu, sigma float64) sizeHistogram {
	if n == 1 {
		return []int{min}
	}

	var (
		rng = rand.New(rand.NewPCG(1, 2))
		ln  = &logNormal{mu: mu, sigma: sigma, src: rng}
	)

	samples := make([]float64, n)
	for i := 0; i < n; i++ {
		samples[i] = ln.Rand()
	}

	var (
		ret       = make(sizeHistogram, n)
		minSample = slices.Min(samples)
		maxSample = slices.Max(samples)
	)
	for i := 0; i < n; i++ {
		normalized := (samples[i] - minSample) / (maxSample - minSample)
		ret[i] = min + int(float64(max-min)*normalized)
	}

	slices.Sort(ret)

	return ret
}

type logNormal struct {
	mu    float64
	sigma float64
	src   rand.Source
}

func (ln *logNormal) Rand() float64 {
	var rnd float64
	if ln.src == nil {
		rnd = rand.NormFloat64()
	} else {
		rnd = rand.New(ln.src).NormFloat64()
	}
	return math.Exp(rnd*ln.sigma + ln.mu)
}
