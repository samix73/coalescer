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

type FetchResult[K comparable, V any] = []Result[K, V]

type Result[K comparable, V any] struct {
	Key   K
	Value V
	Err   error
}

type batchRequest[K comparable, V any] struct {
	ctx    context.Context
	keys   []K
	result chan FetchResult[K, V]
}

// Coalescer batches fetch requests that arrive within a specified time window and executes them together using the provided Fetcher function.
type Coalescer[K comparable, V any] struct {
	fetcher Fetcher[K, V]
	window  time.Duration
	pending []batchRequest[K, V]

	mu sync.Mutex
}

// NewCoalescer creates a new Coalescer with the specified time window and fetcher function.
func NewCoalescer[K comparable, V any](window time.Duration, fetcher Fetcher[K, V]) *Coalescer[K, V] {
	return &Coalescer[K, V]{
		window:  window,
		fetcher: fetcher,
	}
}

// Start begins the coalescing process. It should be run in a separate goroutine.
// The coalescer will continue to batch and execute fetch requests until the provided context is cancelled.
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
				results := make(FetchResult[K, V], 0, len(req.keys))
				for _, key := range req.keys {
					results = append(results, Result[K, V]{Key: key, Err: ctx.Err()})
				}
				req.result <- results
				close(req.result)
			}
			return
		}
	}
}

// Flush executes all pending fetch requests immediately, regardless of the time window.
// It should be called when the coalescer is stopped to ensure that all pending requests are processed.
// Context is passed to the fetcher and can be used to cancel the fetch operation if needed. If the context is cancelled, all pending requests will return with the context error.
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

			select {
			case <-req.ctx.Done():
				results := make(FetchResult[K, V], 0, len(req.keys))
				for _, key := range req.keys {
					results = append(results, Result[K, V]{Key: key, Err: req.ctx.Err()})
				}

				req.result <- results

				return
			case <-fetchReady:
				// If the caller's context was also cancelled, prefer the cancellation error.
				select {
				case <-req.ctx.Done():
					results := make(FetchResult[K, V], 0, len(req.keys))
					for _, key := range req.keys {
						results = append(results, Result[K, V]{Key: key, Err: req.ctx.Err()})
					}
					req.result <- results

					return
				default:
				}

				if fetchErr != nil {
					results := make(FetchResult[K, V], 0, len(req.keys))
					for _, key := range req.keys {
						results = append(results, Result[K, V]{Key: key, Err: fetchErr})
					}
					req.result <- results

					return
				}

				results := make(FetchResult[K, V], 0, len(req.keys))
				for _, key := range req.keys {
					if value, ok := fetchResults[key]; ok {
						results = append(results, Result[K, V]{Key: key, Value: value})
					} else {
						results = append(results, Result[K, V]{Key: key, Err: ErrNotFound})
					}
				}

				req.result <- results
			}
		}(req)
	}

	wg.Wait()
}

// Fetch adds a new fetch request to the coalescer.
// It returns the results of the fetch request once it has been processed.
// If the coalescer is stopped before the request is processed, it will return an error.
// If context is cancelled fetch will return immediately with the context error.
// If the fetcher returns an error, it will be returned for all keys in the request.
// Keys that are not found in the fetch results will return ErrNotFound.
func (c *Coalescer[K, V]) Fetch(ctx context.Context, keys ...K) FetchResult[K, V] {
	c.mu.Lock()
	results := make(chan FetchResult[K, V], 1)
	c.pending = append(c.pending, batchRequest[K, V]{
		ctx:    ctx,
		keys:   keys,
		result: results,
	})
	c.mu.Unlock()

	return <-results
}
