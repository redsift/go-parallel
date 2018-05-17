package reducers

import (
	"sync"
)

// FnInt64 is the template for a reducer function that performs
// an associative operation on an int64
type FnInt64 func(p, c int64) int64

type ass struct {
	wg              *sync.WaitGroup
	fn              FnInt64
	initial, result int64
	err             error
}

// Add is p + c
func Add(p, c int64) int64 {
	return p + c
}

// Multiply is p * c
func Multiply(p, c int64) int64 {
	return p * c
}

// Min returns the lesser of p and c
func Min(p, c int64) int64 {
	if c < p {
		return c
	}
	return p
}

// Max returns the greater of p and c
func Max(p, c int64) int64 {
	if c > p {
		return c
	}

	return p
}

// NewAssociativeInt64 returns a helper struct that performs
// map/reduce operations for the int64 type and incorporates
// a wait group to block the caller until all the data has been
// computed.
func NewAssociativeInt64(initial int64, fn FnInt64) *ass {
	var wg sync.WaitGroup
	wg.Add(1)

	return &ass{wg: &wg, initial: initial, fn: fn}
}

// Value returns the initial value
func (a *ass) Value() int64 {
	return a.initial
}

// Reducer returns fn(p, c) of the input
func (a *ass) Reducer() func(interface{}, interface{}) interface{} {
	return func(p interface{}, v interface{}) interface{} {
		if v == nil {
			return p
		}

		return a.fn(p.(int64), v.(int64))
	}
}

// Then is passed to Parallel to signal the completion of the operations
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

// Get waits for all the jobs to be mapped and reduced before
// returning the final value and/or any errors during the operation
func (a *ass) Get() (int64, error) {
	a.wg.Wait()

	return a.result, a.err
}
