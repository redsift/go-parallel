package parallel

import (
	"math"
	"math/rand"
	"testing"

	"fmt"
	"runtime"

	"github.com/redsift/go-parallel/reducers"
)

const runs = 10000000

// BenchmarkNaive performs poorly due to locking in the global rand generator
func BenchmarkNaive(b *testing.B) {
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

// BenchmarkVanilla is the simplest single threaded counter
func BenchmarkVanilla(b *testing.B) {
	localSrc := rand.New(rand.NewSource(int64(42)))

	for n := 0; n < b.N; n++ {
		count := 0

		for i := 0; i < runs; i++ {
			if heads := localSrc.Int() % 2; heads == 0 {
				count++
			}
		}

		won := math.Round(100 * float64(count) / float64(runs))
		if won != 50 {
			b.Error("Unexpected result", won)
		}
	}
}

// BenchmarkWithParallelOverhead is an example of degenerate use of the structure,
// as each goroutine gets a single int to map on each mapper invocation and incurs all
// the overhead of the scattering, channel communication, context switching and gathering
func BenchmarkWithParallelOverhead(b *testing.B) {
	cores := 4
	m, c := OptMappers(cores, func(i int) interface{} {
		return rand.New(rand.NewSource(int64(i)))
	}, nil)
	defer c()

	for n := 0; n < b.N; n++ {
		add := reducers.NewAssociativeInt64(0, reducers.Add)
		q, _ := Parallel(add.Value(), func(src interface{}, _ interface{}) interface{} {
			localSrc := src.(*rand.Rand)
			if heads := localSrc.Int() % 2; heads == 0 {
				return int64(1)
			}

			return nil
		}, add.Reducer(), add.Then(), m)

		for v := 0; v < runs; v++ {
			q <- 1
		}
		close(q)

		count, err := add.Get()
		if err != nil {
			b.Fatal(err)
		}

		won := math.Round(100 * float64(count) / float64(runs))
		if won != 50 {
			b.Error("Unexpected result", won)
		}
	}
}

// BenchmarkWithParallel is an typical use case for small quick compute operations
// as each mapper does a bundle of workunits
func BenchmarkWithParallel(b *testing.B) {

	const workBatchPerMapper = 10

	for cores := 1; cores < runtime.NumCPU()+1; cores++ {
		b.Run(fmt.Sprintf("Cores-%d", cores), func(b *testing.B) {

			m, c := OptMappers(cores, func(i int) interface{} {
				return rand.New(rand.NewSource(int64(i)))
			}, nil)
			defer c()

			for n := 0; n < b.N; n++ {
				add := reducers.NewAssociativeInt64(0, reducers.Add)
				q, err := Parallel(add.Value(), func(src interface{}, j interface{}) interface{} {
					localSrc := src.(*rand.Rand)
					jobs := j.(int)

					count := 0
					for i := 0; i < jobs; i++ {
						if heads := localSrc.Int() % 2; heads == 0 {
							count++
						}
					}

					return int64(count)
				}, add.Reducer(), add.Then(), m)

				if err != nil {
					b.Fatal(err)
				}

				workUnits := cores * workBatchPerMapper
				for v := 0; v < workUnits; v++ {
					q <- runs / workUnits
				}
				close(q)

				count, err := add.Get()
				if err != nil {
					b.Fatal(err)
				}

				won := math.Round(100 * float64(count) / float64(runs))
				if won != 50 {
					b.Error("Unexpected result", won)
				}
			}
		})
	}

}
