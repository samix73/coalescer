[![Go Reference](https://pkg.go.dev/badge/github.com/samix73/coalescer.svg)](https://pkg.go.dev/github.com/samix73/coalescer)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/samix73/coalescer)

# Coalescer

This package provides a generic, time-windowed request coalescer for batched fetches.

It lets multiple callers enqueue key-based lookups and resolves them together in one `fetcher` call per window. The goal is to reduce duplicate work and upstream load (for example: DB/API/cache lookups).

Callers enqueue keys with `Fetch(ctx, keys...)` and receive a per-request result channel. A background loop (`Start(ctx)`) flushes pending requests every `window` duration, de-duplicates keys across all queued requests, invokes `fetcher(ctx, uniqueKeys)` once for the batch, and fans the results back to each request channel.

## API Overview

- `type Fetcher[K comparable, V any] func(ctx context.Context, keys []K) (map[K]V, error)`
  - User-provided batch function.
  - Should return only found keys; missing keys are treated as `ErrNotFound`.

- `type Result[K comparable, V any] struct { Key K; Value V; Err error }`
  - One result per requested key.

- `func NewCoalescer[K comparable, V any](window time.Duration, fetcher Fetcher[K, V]) *Coalescer[K, V]`
  - Creates a coalescer with the given flush window and fetcher.

- `func (c *Coalescer[K, V]) Start(ctx context.Context)`
  - Runs the periodic flush loop.
  - On context cancellation, any queued requests receive `ctx.Err()`.

- `func (c *Coalescer[K, V]) Flush(ctx context.Context)`
  - Immediately flushes pending requests.
  - Useful for tests or low-latency paths.

- `func (c *Coalescer[K, V]) Fetch(ctx context.Context, keys ...K) <-chan Result[K, V]`
  - Enqueues a request and returns a buffered result channel.
  - Channel is closed after all requested keys have a result.

- `var ErrNotFound = errors.New("not found")`
  - Returned per key when fetcher does not include that key in its result map.

## Error Semantics

For each key requested, exactly one `Result` is sent:

- If `fetcher` returns an error, every key in every pending request gets that error.
- If `fetcher` succeeds but omits a key, that key gets `ErrNotFound`.
- If `Start` context is canceled before flush completes for queued items, queued keys get `ctx.Err()`.

## Quick Start

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func main() {
	// Example backing store.
	users := map[int]string{
		1: "Ada",
		2: "Grace",
		3: "Linus",
	}

	fetcher := func(ctx context.Context, keys []int) (map[int]string, error) {
		// In real usage, do one batched DB/API call here.
		out := make(map[int]string, len(keys))
		for _, k := range keys {
			if v, ok := users[k]; ok {
				out[k] = v
			}
		}
		return out, nil
	}

	c := NewCoalescer[int, string](50*time.Millisecond, fetcher)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Start(ctx)

	// Request a small batch from one caller.
	results := c.Fetch(context.Background(), 1, 3, 99)
	for r := range results {
		switch {
		case errors.Is(r.Err, ErrNotFound):
			fmt.Printf("key=%d not found\n", r.Key)
		case r.Err != nil:
			fmt.Printf("key=%d error=%v\n", r.Key, r.Err)
		default:
			fmt.Printf("key=%d value=%q\n", r.Key, r.Value)
		}
	}
}
```

## Coalescing Across Callers

Multiple callers within the same flush window are grouped into one fetch:

```go
chA := c.Fetch(ctx, 1, 2)
chB := c.Fetch(ctx, 2, 3)
// After the next flush, fetcher sees unique keys roughly like: [1,2,3].
```

Both callers still receive results for the keys they asked for.

## Notes and Lifecycle

- Start the background loop once: `go c.Start(ctx)`.
- Keep `ctx` alive while using `Fetch`.
- If you need immediate processing (for example in tests), call `c.Flush(ctx)` directly.
- Avoid enqueuing new `Fetch` requests after canceling `Start` context, unless you plan to flush manually.


## License

MIT. See [LICENSE](LICENSE).
