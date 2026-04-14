package coalescer

import (
	"context"
	"maps"
	"slices"
	"sync"
	"time"
)

type Fetcher[K comparable, V any] func(ctx context.Context, keys []K) (map[K]V, error)

// keyResult holds the outcome for a single key: either a value or an error.
type keyResult[V any] struct {
	value V
	err   error
}

type batchRequest[K comparable, V any] struct {
	ctx    context.Context
	keys   []K
	result chan map[K]keyResult[V]
}

type Coalescer[K comparable, V any] struct {
	fetcher Fetcher[K, V]
	window  time.Duration
	pending []batchRequest[K, V]

	mu sync.Mutex
}

func NewCoalescer[K comparable, V any](window time.Duration, fetcher Fetcher[K, V]) *Coalescer[K, V] {
	return &Coalescer[K, V]{
		window:  window,
		fetcher: fetcher,
	}
}

func (c *Coalescer[K, V]) Start(ctx context.Context) {
	ticker := time.NewTicker(c.window)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.Flush(ctx)
		case <-ctx.Done():
			c.mu.Lock()
			pending := c.pending
			c.pending = nil
			c.mu.Unlock()
			for _, req := range pending {
				results := make(map[K]keyResult[V], len(req.keys))
				for _, key := range req.keys {
					results[key] = keyResult[V]{err: ctx.Err()}
				}
				req.result <- results
				close(req.result)
			}
			return
		}
	}
}

func (c *Coalescer[K, V]) Flush(ctx context.Context) {
	c.mu.Lock()
	pending := c.pending
	c.pending = nil
	c.mu.Unlock()

	if len(pending) == 0 {
		return
	}

	keysSet := make(map[K][]int) // maps key to index of batchRequest in pending
	for i, req := range pending {
		for _, key := range req.keys {
			keysSet[key] = append(keysSet[key], i)
		}
	}

	var (
		fetchResults map[K]V
		fetchErr     error
		fetchReady   = make(chan struct{})
	)

	go func() {
		fetchResults, fetchErr = c.fetcher(ctx, slices.Collect(maps.Keys(keysSet)))
		close(fetchReady)
	}()

	var wg sync.WaitGroup
	for _, req := range pending {
		wg.Add(1)
		go func(req batchRequest[K, V]) {
			defer wg.Done()
			defer close(req.result)

			results := make(map[K]keyResult[V], len(req.keys))

			select {
			case <-req.ctx.Done():
				for _, key := range req.keys {
					results[key] = keyResult[V]{err: req.ctx.Err()}
				}
			case <-fetchReady:
				// If the caller's context was also cancelled, prefer the cancellation error.
				if req.ctx.Err() != nil {
					for _, key := range req.keys {
						results[key] = keyResult[V]{err: req.ctx.Err()}
					}
					req.result <- results
					return
				}

				if fetchErr != nil {
					for _, key := range req.keys {
						results[key] = keyResult[V]{err: fetchErr}
					}
					req.result <- results
					return
				}

				for _, key := range req.keys {
					if v, ok := fetchResults[key]; ok {
						results[key] = keyResult[V]{value: v}
					}
				}
			}

			req.result <- results
		}(req)
	}

	wg.Wait()
}

// Fetch enqueues a request for the given keys and blocks until the coalescer
// delivers the result. It returns a map of found keys to their values. Keys
// absent from the fetcher's response are simply absent from the returned map.
// If any key encounters an error (e.g. context cancellation or a fetcher error),
// Fetch returns nil and that error; within a single batch all keys share the
// same error so only one error value is possible per call.
func (c *Coalescer[K, V]) Fetch(ctx context.Context, keys ...K) (map[K]V, error) {
	c.mu.Lock()
	ch := make(chan map[K]keyResult[V], 1)
	c.pending = append(c.pending, batchRequest[K, V]{
		ctx:    ctx,
		keys:   keys,
		result: ch,
	})
	c.mu.Unlock()

	items := <-ch
	values := make(map[K]V, len(items))
	for k, item := range items {
		if item.err != nil {
			return nil, item.err
		}
		values[k] = item.value
	}
	return values, nil
}
