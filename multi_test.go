package parallel

import (
	"math"
	"math/rand"
	"testing"

	"github.com/redsift/go-parallel/reducers"
)

const runs = 10000000

func BenchmarkVanilla(b *testing.B) {
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

func BenchmarkWithParallelOverhead(b *testing.B) {
	cores := 4
	m, c := OptMappers(cores, func(i int) interface{} {
		return rand.New(rand.NewSource(int64(i)))
	}, nil)
	defer c()

	for n := 0; n < b.N; n++ {
		add := reducers.NewAssociativeInt64(0, reducers.Add)
		q := Parallel(add.Value(), func(src interface{}, _ interface{}) interface{} {
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

func BenchmarkWithParallel(b *testing.B) {
	cores := 4
	m, c := OptMappers(cores, func(i int) interface{} {
		return rand.New(rand.NewSource(int64(i)))
	}, nil)
	defer c()

	for n := 0; n < b.N; n++ {
		add := reducers.NewAssociativeInt64(0, reducers.Add)
		q := Parallel(add.Value(), func(src interface{}, j interface{}) interface{} {
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

		for v := 0; v < cores; v++ {
			q <- runs / cores
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
