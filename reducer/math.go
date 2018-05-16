package reducer

import (
	"sync"
)

type FnInt64 func(p, c int64) int64

type ass struct {
	wg *sync.WaitGroup
	fn FnInt64
	initial, result int64
	err error
}

func Add(p, c int64) int64 {
	return p + c
}

func Multiply(p, c int64) int64 {
	return p * c
}

func Min(p, c int64) int64 {
	if c < p {
		return c
	}
	return p
}

func Max(p, c int64) int64 {
	if c > p {
		return c
	}
	return p
}

func NewAssociativeInt64(initial int64, fn FnInt64) *ass {
	var wg sync.WaitGroup
	wg.Add(1)

	return &ass{wg: &wg, initial: initial, fn: fn}
}

func (a *ass) Value() int64 {
	return a.initial
}

func (a *ass) Init() func(int) interface{} {
	return nil
}

func (a *ass) Reducer() func(interface{}, interface{}) interface{} {
	return func(p interface{}, v interface{}) interface{} {
		if v == nil {
			return p
		}

		return a.fn(p.(int64), v.(int64))
	}
}

func (a *ass) Then() func(interface{}, error) {
	return func(t interface{}, err error) {
		defer a.wg.Done()

		if err != nil {
			a.err = err
			return
		}

		a.result = t.(int64)
	}
}

func (a *ass) Get() (int64, error) {
	a.wg.Wait()

	return a.result, a.err
}