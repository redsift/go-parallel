# Parallel

[![Go Report Card](https://goreportcard.com/badge/github.com/redsift/go-parallel)](https://goreportcard.com/report/github.com/redsift/go-parallel)
[![Release](https://img.shields.io/github/release/redsift/go-parallel/all.svg)](https://github.com/redsift/go-parallel/releases)
[![CircleCI](https://circleci.com/gh/redsift/go-parallel.svg?style=shield)](https://circleci.com/gh/redsift/go-parallel)

Dependency free micro library for map/reduce and parallel loops using go
routines and channels.

## Features

Supports contexts (i.e. timeouts and cancellation), panic trapping and
one time go routine initialization.

## Usage

```
// Parallel performs a map/reduce using go routines and channels
//
// value: is the initial value of the reducer i.e. the first `previous` for the reducer
// mapper: functions are called in multiple goroutines, they consume jobs and returns `current`
// for the reducer
// reducer: functions are called synchronously and returns the value for `previous` for the
// next invocation
// then: receives the last output produced by the reducer
// opts: control context, queue sizes, goroutine pool & `init` values for mappers
//
// The returned channel is the job queue and must be closed by the caller when all jobs have
// been submitted

func Parallel(value interface{},
	mapper func(init interface{}, job interface{}) interface{},
	reducer func(previous interface{}, current interface{}) interface{},
	then func(final interface{}, err error),
	opts ...Option) (chan interface{}, error)
```

The pipeline is derived from a standard map/reduce structure where the `mapper`
takes a task and produces an output and the `reducer` combines those outputs
where the initial state of the reduction operation is set by `value`. `reducer`
calls are guaranteed to be serial though order is not specified so operations
can/should be lock less but must be associative. `mapper` calls are
executed on any number of goroutines and if required, local context can
be managed for expensive initialization operations though the user of a
non default `Option`.

`then` is final result of all the associatively performed `reducer` operations.
Any errors during map or reduce operations will be returned to the `then` function.

Reference [TestNetworkRequestsInParallel](https://github.com/redsift/go-parallel/blob/master/network_test.go#L137-L173) for an representative example. The
use of parallel network calls via `Parallel` reduce average test time
in this instance by **~13x**.
```
=== RUN   TestNetworkRequestsSerially
--- PASS: TestNetworkRequestsSerially (26.50s)
	network_test.go:134: [https://facebook.com/ = TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256 ...
=== RUN   TestNetworkRequestsInParallel
--- PASS: TestNetworkRequestsInParallel (2.08s)
	network_test.go:172: [https://wikipedia.org/ = TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305 ...
```

## Performance

While this library is typically used for mapping operations that are in
the order of milliseconds e.g. network requests, it can be used for
compute heavy workloads with the right approach.

### Sample performance on an 8 core macOS desktop
```
$ go test -bench=.
goos: darwin
goarch: amd64
pkg: github.com/redsift/go-parallel
BenchmarkNaive-8                  	       5	 235266212 ns/op
BenchmarkVanilla-8                	      10	 151033909 ns/op
BenchmarkWithParallelOverhead-8   	       1	5728391114 ns/op
BenchmarkWithParallel/Cores-1-8   	      10	 154605812 ns/op
BenchmarkWithParallel/Cores-2-8   	      20	  78491720 ns/op
BenchmarkWithParallel/Cores-3-8   	      20	  51968062 ns/op
BenchmarkWithParallel/Cores-4-8   	      30	  42213590 ns/op
BenchmarkWithParallel/Cores-5-8   	      30	  37917650 ns/op
BenchmarkWithParallel/Cores-6-8   	      50	  33460789 ns/op
BenchmarkWithParallel/Cores-7-8   	      50	  31308671 ns/op
BenchmarkWithParallel/Cores-8-8   	      50	  29503476 ns/op
BenchmarkSimple-8                 	  200000	     10670 ns/op
BenchmarkMappers-8                	  200000	      7484 ns/op
PASS
ok  	github.com/redsift/go-parallel	24.381s
```

`BenchmarkVanilla-8` and `BenchmarkWithParallel/Cores-1-8` are equivalent
as both use a lock less `rand` source to generate random numbers and count
the output using 1 core. Using the `Parallel` structure imposes a 2% overhead
when the work is split into 10 batches per mapper. This reduces to ~0.2% if
`workBatchPerMapper` is reduced to 1.

However, once we allow more cores to perform mapping operations in this
use case we see a steady improvement in performance with 2 cores operating
1.96x faster and 8 cores running 5.24x faster.


## TODO
- Add ex repo with job distribution.
- Wait for typing system to avoid ugly casts.