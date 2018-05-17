package reducers

import "sync"

type lss struct {
	wg              *sync.WaitGroup
	initial, result []string
	err             error
}

// NewStringList append the string output of a mapper to form a []string
func NewStringList(capacity int) *lss {
	var wg sync.WaitGroup
	wg.Add(1)

	return &lss{wg: &wg, initial: make([]string, 0, capacity)}
}

// Value returns the initial value
func (a *lss) Value() []string {
	return a.initial
}

// Reducer returns an append of the input
func (a *lss) Reducer() func(interface{}, interface{}) interface{} {
	return func(p interface{}, v interface{}) interface{} {
		return append(p.([]string), v.(string))
	}
}

// Then is passed to Parallel to signal the completion of the operations
func (a *lss) Then() func(interface{}, error) {
	return func(t interface{}, err error) {
		defer a.wg.Done()

		if err != nil {
			a.err = err
			return
		}

		a.result = t.([]string)
	}
}

// Get waits for all the jobs to be mapped and reduced before
// returning the final value and/or any errors during the operation
func (a *lss) Get() ([]string, error) {
	a.wg.Wait()

	return a.result, a.err
}
