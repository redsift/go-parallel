// Package parallel provides an implementation of map/reduce using channels and go routines.
package parallel

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

var (
	// ErrOptInvalidValueQueue indicates the option supplied to set the size of the inbound job queue is invalid
	ErrOptInvalidValueQueue = errors.New("invalid option value: queue")

	// ErrOptInvalidValueContext indicates the option supplied to set the context for the parallel operation is invalid
	ErrOptInvalidValueContext = errors.New("invalid option value: context")

	// ErrCancelledMapper indicates that the mapper option has been reused after being cancelled
	ErrCancelledMapper = errors.New("mapper was already cancelled")
)

// ErrTrappedPanic wraps an underlying panic and call stack for
// a panic that was trapped during mapping or reduction
type ErrTrappedPanic struct {
	Panic interface{}
	Stack []byte
}

func (e ErrTrappedPanic) Error() string {
	return fmt.Sprint(e.Panic, "\n", string(e.Stack))
}

type mapper struct {
	count int

	parallel chan mapperOp

	trapped atomic.Value
}

type options struct {
	queue  int
	ctx    context.Context
	mapper *mapper
	// cancel function for mapper, set if default mapper is used
	cancel CancelFunc
}

// Option encapsulate all available options for the Parallel operation
type Option func(*options) error

// OptQueue defaults to the with of the Parallel operation but may be set to a larger value
// to allow the job submission operations to buffer further
func OptQueue(sz int) Option {
	return func(o *options) error {
		if sz < 1 {
			return ErrOptInvalidValueQueue
		}
		o.queue = sz
		return nil
	}
}

// OptContext can be used to supply a context for a Parallel operation, typically
// to affect timeout and provide cancellation
func OptContext(ctx context.Context) Option {
	return func(o *options) error {
		if ctx == nil {
			return ErrOptInvalidValueContext
		}
		o.ctx = ctx
		return nil
	}
}

type mapperOp struct {
	fn       func(interface{}, interface{}) interface{}
	in, out  chan interface{}
	wg       *sync.WaitGroup
	cls, clx <-chan struct{}
}

// CancelFunc tells a mapper to shut down any worker routines
type CancelFunc func()

// OptMappers can be used to control the number of go routines used to run mappers
// (defaults to runtime.NumCPU()) and supply `init` and `destroy` hooks for the routines
func OptMappers(sz int, init func(int) interface{}, destroy func(interface{})) (Option, CancelFunc) {
	if sz < 1 {
		sz = runtime.NumCPU() // cant change after process is started
	}
	m, c := newMapper(sz, init, destroy)

	return func(o *options) error {
		o.mapper = m

		return nil
	}, c
}

func newMapper(sz int, init func(int) interface{}, destroy func(interface{})) (*mapper, CancelFunc) {
	m := &mapper{count: sz, parallel: make(chan mapperOp, sz)}

	for i := 0; i < m.count; i++ {
		var s interface{}
		if init != nil {
			s = init(i)
		}

		// call_map
		go func(i int, s interface{}) {
			var in chan interface{}
			var wg *sync.WaitGroup
			defer func() {
				if r := recover(); r != nil {
					err := ErrTrappedPanic{r, debug.Stack()}
					m.trapped.Store(err)
				}

				if wg != nil {
					wg.Done()
				}

				// drain the in channel as we don't want the writer to
				// block
				if in != nil {
					for _ = range in {
					}
				}

				if destroy != nil {
					destroy(s)
				}
			}()

			for op := range m.parallel {

				wg = op.wg
				in = op.in
			loop:
				for {
					select {
					case <-op.clx:
						break loop
					case <-op.cls:
						for _ = range in {
						}
						break loop

					case j, ok := <-in:
						if !ok {
							break loop
						}
						op.out <- op.fn(s, j)
					}
				}

				wg.Done()
				wg = nil
				in = nil
			}
		}(i, s)
	}

	return m, func() {
		m.trapped.Store(ErrTrappedPanic{Panic: ErrCancelledMapper})
		close(m.parallel)
	}
}

func makeOptions(opts []Option) (*options, error) {
	o := options{
		ctx: context.Background(),
	}

	for _, opt := range opts {
		err := opt(&o)
		if err != nil {
			return nil, err
		}
	}

	// no mapper supplied, make a new one on every invocation
	if o.mapper == nil {
		m, c := newMapper(runtime.NumCPU(), nil, nil)

		o.mapper = m
		o.cancel = c
	}

	if o.queue == 0 {
		o.queue = o.mapper.count
	}

	return &o, nil
}

// Parallel performs a map/reduce using go routines and channels
//
// value: is the initial value of the reducer i.e. the first `previous` for the reducer
// mapper: functions are called in multiple goroutines, they consume jobs and returns `current` for the reducer
// reducer: functions are called synchronously and returns the value for `previous` for the next invocation
// then: receives the last output produced by the reducer
// opts: control context, queue sizes, goroutine pool & `init` values for mappers
//
// The returned channel is the job queue and must be closed by the caller when all jobs have been submitted
func Parallel(value interface{},
	mapper func(init interface{}, job interface{}) interface{},
	reducer func(previous interface{}, current interface{}) interface{},
	then func(final interface{}, err error),
	opts ...Option) (chan interface{}, error) {

	o, err := makeOptions(opts)
	if err != nil {
		return nil, err
	}

	in := make(chan interface{}, o.queue)
	out := make(chan interface{}, o.mapper.count)

	var trapped atomic.Value

	var wg sync.WaitGroup
	wg.Add(o.mapper.count)

	var wo sync.WaitGroup
	wo.Add(1)

	t := value

	// call_then
	go func() {
		defer func() {
			if o.cancel != nil {
				o.cancel()
			}
		}()
		wg.Wait()
		close(out)

		if then != nil {
			wo.Wait()

			terr := trapped.Load()

			if err := o.mapper.trapped.Load(); err != nil {
				then(nil, err.(ErrTrappedPanic))
			} else if terr != nil {
				then(nil, terr.(ErrTrappedPanic))
			} else if err := o.ctx.Err(); err != nil {
				then(nil, err)
			} else {
				then(t, err)
			}
		}
	}()

	cls := o.ctx.Done()
	clx := make(chan struct{})
	if reducer != nil {

		// call_reduce
		go func() {
			defer func() {
				if r := recover(); r != nil {
					err := ErrTrappedPanic{r, debug.Stack()}

					trapped.Store(err)
				}
				wo.Done()

				// at this point the map operations might be stuck
				// writing so signal them to close using clx and drain
				// the out channel to unblock them
				close(clx)
				for _ = range out {
				}
			}()
			for a := range out {
				t = reducer(t, a)
			}
		}()
	}

	if err := o.mapper.trapped.Load(); err != nil {
		panic(err) // can't reuse after panic TODO: Docs
	}

	m := mapperOp{mapper, in, out, &wg, cls, clx}
	for i := 0; i < o.mapper.count; i++ {
		o.mapper.parallel <- m
	}

	return in, nil
}
