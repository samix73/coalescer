package coalescer

import (
	"context"
	"errors"
	"maps"
	"slices"
	"sync"
	"time"
)

var (
	ErrNotFound = errors.New("not found")
)

type Fetcher[K comparable, V any] func(ctx context.Context, keys []K) (map[K]V, error)

type Result[K comparable, V any] struct {
	Values map[K]V
	Errors map[K]error
}

type batchRequest[K comparable, V any] struct {
	ctx    context.Context
	keys   []K
	result chan Result[K, V]
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
				errs := make(map[K]error, len(req.keys))
				for _, key := range req.keys {
					errs[key] = ctx.Err()
				}
				req.result <- Result[K, V]{Values: make(map[K]V), Errors: errs}
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

			errs := make(map[K]error, len(req.keys))
			values := make(map[K]V, len(req.keys))

			select {
			case <-req.ctx.Done():
				for _, key := range req.keys {
					errs[key] = req.ctx.Err()
				}
			case <-fetchReady:
				// If the caller's context was also cancelled, prefer the cancellation error.
				select {
				case <-req.ctx.Done():
					for _, key := range req.keys {
						errs[key] = req.ctx.Err()
					}
					req.result <- Result[K, V]{Values: values, Errors: errs}
					return
				default:
				}

				if fetchErr != nil {
					for _, key := range req.keys {
						errs[key] = fetchErr
					}
					req.result <- Result[K, V]{Values: values, Errors: errs}
					return
				}

				for _, key := range req.keys {
					if value, ok := fetchResults[key]; ok {
						values[key] = value
					} else {
						errs[key] = ErrNotFound
					}
				}
			}

			req.result <- Result[K, V]{Values: values, Errors: errs}
		}(req)
	}

	wg.Wait()
}

func (c *Coalescer[K, V]) Fetch(ctx context.Context, keys ...K) <-chan Result[K, V] {
	c.mu.Lock()
	defer c.mu.Unlock()

	results := make(chan Result[K, V], 1)
	c.pending = append(c.pending, batchRequest[K, V]{
		ctx:    ctx,
		keys:   keys,
		result: results,
	})

	return results
}
