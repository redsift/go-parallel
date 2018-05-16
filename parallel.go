package parallel

import (
	"sync"
	"runtime"
	"errors"
	"context"
	"fmt"
	"sync/atomic"
	"runtime/debug"
)

var (
	ErrOptInvalidValueCount = errors.New("invalid option value: count")
	ErrOptInvalidValueQueue = errors.New("invalid option value: queue")
	ErrOptInvalidValueContext = errors.New("invalid option value: context")
)

type ErrTrappedPanic struct {
	Panic interface{}
	Stack []byte
}

func (e ErrTrappedPanic) Error() string {
	return fmt.Sprint(e.Panic, "\n", string(e.Stack))
}

type options struct {
	count int
	queue int
	ctx	  context.Context
}

type Option func (*options) error

func OptCount(sz int) Option {
	return func (o* options) error {
		if sz < 1 {
			return ErrOptInvalidValueCount
		}
		o.count = sz
		return nil
	}
}

func OptQueue(sz int) Option {
	return func (o* options) error {
		if sz < 1 {
			return ErrOptInvalidValueQueue
		}
		o.queue = sz
		return nil
	}
}

func OptContext(ctx context.Context) Option {
	return func (o* options) error {
		if ctx == nil {
			return ErrOptInvalidValueContext
		}
		o.ctx = ctx
		return nil
	}
}


// size, timeout, cache go routines, handle panic (return err)
func Parallel(	value 	interface{},
				init 	func(int) interface{},
				mapper 	func(interface{}, interface{}) interface{},
				reducer func(interface{}, interface{}) interface{},
				then 	func(interface{}, error),
				opts ...Option) chan interface{}  {

	cpus := runtime.NumCPU()
	o := options{
		count: 	cpus,
		queue: 	cpus,
		ctx: 	context.Background(),
	}

	for _, opt := range opts {
		err := opt(&o)
		if err != nil {
			if then != nil {
				then(nil, err)
			}
			return nil
		}
	}

	in := make(chan interface{}, o.queue)
	out := make(chan interface{}, o.count)

	var trapped atomic.Value

	var wg sync.WaitGroup
	wg.Add(o.count)

	var wo sync.WaitGroup
	wo.Add(1)

	t := value

	go func() {
		wg.Wait()
		close(out)

		if then != nil {
			wo.Wait()

			terr := trapped.Load()
			if terr != nil {
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
				for _ = range out { }
			}()
			for a := range out {
				t = reducer(t, a)
			}
		}()
	}


	for i := 0; i < o.count; i++ {
		go func(i int) {
			defer func() {
				if r := recover(); r != nil {
					err := ErrTrappedPanic{r, debug.Stack()}
					trapped.Store(err)
				}
				wg.Done()

				// drain the in channel as we don't want the writer to
				// block
				for _ = range in {}
			}()
			var s interface{}
			if init != nil {
				s = init(i)
			}

			for {
				select {
				case <- clx:
					return
				case <- cls:
					return

				case j, ok := <- in:
					if !ok {
						return
					}
					out <- mapper(s, j)
				}
			}

			for j := range in {
				a := mapper(s, j)

				out <- a
			}
		}(i)
	}

	return in
}