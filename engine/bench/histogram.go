package main

import (
	"fmt"
	"math"
	"strings"
)

var bucketLimits = []float64{
	1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 18, 20, 25, 30, 35, 40, 45,
	50, 60, 70, 80, 90, 100, 120, 140, 160, 180, 200, 250, 300, 350, 400, 450,
	500, 600, 700, 800, 900, 1000, 1200, 1400, 1600, 1800, 2000, 2500, 3000,
	3500, 4000, 4500, 5000, 6000, 7000, 8000, 9000, 10000, 12000, 14000,
	16000, 18000, 20000, 25000, 30000, 35000, 40000, 45000, 50000, 60000,
	70000, 80000, 90000, 100000, 120000, 140000, 160000, 180000, 200000,
	250000, 300000, 350000, 400000, 450000, 500000, 600000, 700000, 800000,
	900000, 1000000, 1200000, 1400000, 1600000, 1800000, 2000000, 2500000,
	3000000, 3500000, 4000000, 4500000, 5000000, 6000000, 7000000, 8000000,
	9000000, 10000000, 12000000, 14000000, 16000000, 18000000, 20000000,
	25000000, 30000000, 35000000, 40000000, 45000000, 50000000, 60000000,
	70000000, 80000000, 90000000, 100000000, 120000000, 140000000, 160000000,
	180000000, 200000000, 250000000, 300000000, 350000000, 400000000,
	450000000, 500000000, 600000000, 700000000, 800000000, 900000000,
	1000000000, 1200000000, 1400000000, 1600000000, 1800000000, 2000000000,
	2500000000.0, 3000000000.0, 3500000000.0, 4000000000.0, 4500000000.0,
	5000000000.0, 6000000000.0, 7000000000.0, 8000000000.0, 9000000000.0,
	1e200,
}

type histogram struct {
	numBuckets int
	buckets    []float64
	min        float64
	max        float64
	num        float64
	sum        float64
	sumSquares float64
}

func newHistogram() *histogram {
	return &histogram{
		numBuckets: len(bucketLimits),
		buckets:    make([]float64, len(bucketLimits)),
		min:        math.MaxFloat64,
	}
}

func (h *histogram) Add(value float64) {
	if value > h.max {
		h.max = value
	}
	if value < h.min {
		h.min = value
	}

	h.num++
	h.sum += value
	h.sumSquares += (value * value)

	var i int
	for i < h.numBuckets-1 && bucketLimits[i] <= value {
		i++
	}
	h.buckets[i]++
}

func (h *histogram) ToString() (r string) {
	r += "-----------------------------------------\n"
	mult := 100.0 / h.num
	var sum float64
	for i := 0; i < h.numBuckets; i++ {
		if h.buckets[i] <= 0.0 {
			continue
		}
		sum += h.buckets[i]

		var left float64
		if i > 0 {
			left = bucketLimits[i-1]
		}
		r += fmt.Sprintf("[ %7.0f, %7.0f ) %7.0f %7.3f%% %7.3f%% ",
			left, bucketLimits[i], h.buckets[i], mult*h.buckets[i], mult*sum)

		marks := (int)(20*(h.buckets[i]/h.num) + 0.5)
		r += strings.Repeat("#", marks)
		r += "\n"
	}

	return
}
