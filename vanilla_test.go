package parallel

import (
	"testing"
	"math/rand"
	"math"
)

const runs = 10000000

func Benchmark1Vanilla(b *testing.B) {
	for n := 0; n < b.N; n++ {
		count := 0

		for i := 0; i < runs; i++ {
			if heads := rand.Int() % 2; heads == 0 {
				count++
			}
		}

		won := math.Round(100 * float64(count) / float64(runs))
		if won != 50 {
			b.Error("Unexpected result", won)
		}
	}
}
