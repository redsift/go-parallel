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
	ErrOptInvalidValueQueue   = errors.New("invalid option value: queue")
	ErrOptInvalidValueContext = errors.New("invalid option value: context")
	ErrCancelledMapper        = errors.New("mapper was already cancelled")
)

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

type Option func(*options) error

func OptQueue(sz int) Option {
	return func(o *options) error {
		if sz < 1 {
			return ErrOptInvalidValueQueue
		}
		o.queue = sz
		return nil
	}
}

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

// CancelFunc tells a mapper to shut down any worker routines
type CancelFunc func()

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

// Parallel performs a map/reduce using go routines and channels
//
// `value` is the initial value of the reducer i.e. the first `previous` for the reducer
// `mapper` functions are called in multiple goroutines, they consume jobs and returns `current` for the reducer
// `reducer` functions are called synchronously and returns the value for `previous` for the next invocation
// `then` receives the last output produced by the reducer
// `opts` control context, queue sizes, goroutine pool & `init` values for mappers
//
// The returned channel is the job queue and must be closed by the caller when all jobs have been submitted
func Parallel(value interface{},
	mapper func(init interface{}, job interface{}) interface{},
	reducer func(previous interface{}, current interface{}) interface{},
	then func(final interface{}, err error),
	opts ...Option) (chan interface{}, error) {

	cpus := runtime.NumCPU()
	o := options{
		queue: cpus,
		ctx:   context.Background(),
	}

	for _, opt := range opts {
		err := opt(&o)
		if err != nil {
			return nil, err
		}
	}

	// no mapper supplied, make a new one on every invocation
	if o.mapper == nil {
		m, c := newMapper(cpus, nil, nil)

		o.mapper = m
		o.cancel = c
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
