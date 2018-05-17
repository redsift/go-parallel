package parallel

import (
	"testing"
	"math"
	"math/rand"
	"github.com/redsift/go-parallel/reducers"
)

func Benchmark2WithParallel(b *testing.B) {
	m, c := OptMappers(0, func(i int) interface{} {
		return rand.New(rand.NewSource(int64(i)))
	}, nil)
	defer c()

	for n := 0; n < b.N; n++ {
		add := reducers.NewAssociativeInt64(0, reducers.Add)
		q := Parallel(add.Value(), func (src interface{}, j interface{}) interface{} {
			localSrc := src.(*rand.Rand)
			if heads := localSrc.Int() % 2; heads == 0 {
				return int64(1)
			}
			return nil
		}, add.Reducer(), add.Then(), m)

		for v := 0; v < runs; v++ {
			q <- v
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
