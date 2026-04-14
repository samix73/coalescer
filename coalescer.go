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
	Key   K
	Value V
	Err   error
}

type batchRequest[K comparable, V any] struct {
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
	timer := time.NewTimer(c.window)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			c.Flush(ctx)
		case <-ctx.Done():
			c.mu.Lock()
			pending := c.pending
			c.pending = nil
			c.mu.Unlock()
			for _, req := range pending {
				for _, key := range req.keys {
					req.result <- Result[K, V]{Key: key, Err: ctx.Err()}
				}
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

	results, err := c.fetcher(ctx, slices.Collect(maps.Keys(keysSet)))
	if err != nil {
		for _, req := range pending { // if fetcher fails, we need to send the error to all pending requests
			for _, key := range req.keys {
				req.result <- Result[K, V]{
					Key: key,
					Err: err,
				}
			}
			close(req.result)
		}

		return
	}

	for key, value := range results {
		requestsIndex, ok := keysSet[key]
		if !ok {
			continue // this should not happen, but just in case
		}

		for _, reqIndex := range requestsIndex {
			req := pending[reqIndex]
			req.result <- Result[K, V]{
				Key:   key,
				Value: value,
				Err:   nil,
			}
		}
	}

	// Scan for not found keys and send an error for them
	for key, keyIndexes := range keysSet {
		if _, found := results[key]; found {
			continue
		}

		for _, reqIndex := range keyIndexes {
			req := pending[reqIndex]
			req.result <- Result[K, V]{
				Key: key,
				Err: ErrNotFound,
			}
		}
	}

	for _, req := range pending {
		close(req.result)
	}
}

func (c *Coalescer[K, V]) Fetch(keys ...K) <-chan Result[K, V] {
	c.mu.Lock()
	defer c.mu.Unlock()

	results := make(chan Result[K, V], len(keys))
	c.pending = append(c.pending, batchRequest[K, V]{
		keys:   keys,
		result: results,
	})

	return results
}
