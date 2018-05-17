package parallel

import (
	"testing"
	"sync"
	"github.com/redsift/go-parallel/reducers"
	"context"
	"math"
	"github.com/redsift/go-parallel/mappers"
)

func TestExplicit(t *testing.T) {
	var and sync.WaitGroup
	and.Add(1)

	var total int

	q := Parallel(0, func(_ interface{}, j interface{}) interface{} {
		return j
	}, func(t interface{}, a interface{}) interface{} {
		return t.(int) + a.(int)
	}, func(t interface{}, err error) {
		total = t.(int)
		and.Done()
	})

	for _, v := range []int{0, 1, 2, -1} {
		q <- v
	}
	close(q)

	and.Wait()

	if total != 2 {
		t.Error("total incorrect", total)
	}
}


func TestReducerAdd(t *testing.T) {
	add := reducers.NewAssociativeInt64(0, reducers.Add)
	
	q := Parallel(add.Value(), mappers.Noop, add.Reducer(), add.Then())

	for _, v := range []int64{0, 1, 2, -1} {
		q <- v
	}
	close(q)

	total, err := add.Get()
	if err != nil {
		t.Fatal(err)
	}

	if total != 2 {
		t.Error("total incorrect", total)
	}
}

func TestReducerMult1(t *testing.T) {
	add := reducers.NewAssociativeInt64(0, reducers.Multiply)

	q := Parallel(add.Value(),func(_ interface{}, j interface{}) interface{} {
		return j
	}, add.Reducer(), add.Then())

	for _, v := range []int64{0, 1, 2, -1} {
		q <- v
	}
	close(q)

	total, err := add.Get()
	if err != nil {
		t.Fatal(err)
	}

	if total != 0 {
		t.Error("total incorrect", total)
	}
}

func TestReducerMult2(t *testing.T) {
	add := reducers.NewAssociativeInt64(1, reducers.Multiply)

	q := Parallel(add.Value(), func(_ interface{}, j interface{}) interface{} {
		return j
	}, add.Reducer(), add.Then())

	for _, v := range []int64{1, 2, -1} {
		q <- v
	}
	close(q)

	total, err := add.Get()
	if err != nil {
		t.Fatal(err)
	}

	if total != -2 {
		t.Error("total incorrect", total)
	}
}

func TestReducerMin(t *testing.T) {
	add := reducers.NewAssociativeInt64(math.MaxInt64, reducers.Min)

	q := Parallel(add.Value(), func(_ interface{}, j interface{}) interface{} {
		return j
	}, add.Reducer(), add.Then())

	for _, v := range []int64{0, 1, -22, -1} {
		q <- v
	}
	close(q)

	total, err := add.Get()
	if err != nil {
		t.Fatal(err)
	}

	if total != -22 {
		t.Error("total incorrect", total)
	}
}

func TestReducerMax(t *testing.T) {
	add := reducers.NewAssociativeInt64(math.MinInt64, reducers.Max)

	q := Parallel(add.Value(), func(_ interface{}, j interface{}) interface{} {
		return j
	}, add.Reducer(), add.Then())

	for _, v := range []int64{0, 1, -22, -1} {
		q <- v
	}
	close(q)

	total, err := add.Get()
	if err != nil {
		t.Fatal(err)
	}

	if total != 1 {
		t.Error("total incorrect", total)
	}
}

func TestCancel(t *testing.T) {
	var and sync.WaitGroup
	and.Add(1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var total int

	q := Parallel(0, func(_ interface{}, j interface{}) interface{} {
		return j
	}, func(t interface{}, a interface{}) interface{} {
		return t.(int) + a.(int)
	}, func(v interface{}, err error) {
		if v != nil {
			total = v.(int)
		}

		if err != context.Canceled {
			t.Error("unexpected error", err)
		}
		and.Done()
	}, OptContext(ctx))

	for i := 0; i < 1000; i++ {
		q <- i
	}
	close(q)

	and.Wait()

	if total != 0 {
		t.Error("total incorrect", total)
	}
}


func TestPanicMap(t *testing.T) {

	var and sync.WaitGroup
	and.Add(1)


	q := Parallel(0, func(_ interface{}, j interface{}) interface{} {
		panic("junk")
		return j
	}, func(t interface{}, a interface{}) interface{} {
		return t.(int) + a.(int)
	}, func(_ interface{}, err error) {
		if pnk := err.(ErrTrappedPanic).Panic; pnk != "junk" {
			t.Error("unexpected value trapped", pnk)
		}
		and.Done()
	})

	for _, v := range []int{0, 1, 2, -1} {
		q <- v
	}
	close(q)

	and.Wait()
}


func TestPanicReduce(t *testing.T) {

	var and sync.WaitGroup
	and.Add(1)


	q := Parallel(0, func(_ interface{}, j interface{}) interface{} {
		return j
	}, func(t interface{}, a interface{}) interface{} {
		panic("junk")
		return t.(int) + a.(int)
	}, func(_ interface{}, err error) {
		if pnk := err.(ErrTrappedPanic).Panic; pnk != "junk" {
			t.Error("unexpected value trapped", pnk)
		}
		and.Done()
	})

	for _, v := range []int{0, 1, 2, -1} {
		q <- v
	}
	close(q)

	and.Wait()
}

func BenchmarkSimple(b *testing.B) {
	var and sync.WaitGroup

	for n := 0; n < b.N; n++ {
		and.Add(1)

		var total int

		q := Parallel(0,
		func(_ interface{}, j interface{}) interface{} {
			return j
		}, func(p interface{}, a interface{}) interface{} {
			return p.(int) + a.(int)
		}, func(t interface{}, err error) {
			total = t.(int)
			and.Done()
		})

		for _, v := range []int{0, 1, 2, -1} {
			q <- v
		}
		close(q)

		and.Wait()

		if total != 2 {
			b.Error("total incorrect", total, b.N)
		}
	}
}

// Test the performance with reused mappers
func BenchmarkMappers(b *testing.B) {
	var and sync.WaitGroup

	m, c := OptMappers(0, nil, nil)
	defer c()

	for n := 0; n < b.N; n++ {
		and.Add(1)

		var total int

		q := Parallel(0,
			func(_ interface{}, j interface{}) interface{} {
				return j
			}, func(p interface{}, a interface{}) interface{} {
				return p.(int) + a.(int)
			}, func(t interface{}, err error) {
				total = t.(int)
				and.Done()
			}, m)

		for _, v := range []int{0, 1, 2, -1} {
			q <- v
		}
		close(q)

		and.Wait()

		if total != 2 {
			b.Error("total incorrect", total, b.N)
		}
	}
}