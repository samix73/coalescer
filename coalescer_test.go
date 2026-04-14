package coalescer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// happyFetcher returns the keys as both key and value (string → string).
func happyFetcher(_ context.Context, keys []string) (map[string]string, error) {
	result := make(map[string]string, len(keys))
	for _, k := range keys {
		result[k] = k
	}
	return result, nil
}

// slowFetcher blocks for the given duration before returning.
func slowFetcher(d time.Duration) Fetcher[string, string] {
	return func(_ context.Context, keys []string) (map[string]string, error) {
		time.Sleep(d)
		result := make(map[string]string, len(keys))
		for _, k := range keys {
			result[k] = k
		}
		return result, nil
	}
}

// errorFetcher always returns an error.
func errorFetcher(fetchErr error) Fetcher[string, string] {
	return func(_ context.Context, _ []string) (map[string]string, error) {
		return nil, fetchErr
	}
}

// TestFetch_HappyPath verifies that a normal fetch delivers the expected values.
func TestFetch_HappyPath(t *testing.T) {
	c := NewCoalescer[string, string](10*time.Millisecond, happyFetcher)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go c.Start(ctx)

	values, err := c.Fetch(context.Background(), "a", "b", "c")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, key := range []string{"a", "b", "c"} {
		if v, ok := values[key]; !ok || v != key {
			t.Errorf("key %q: want value %q, got %q", key, key, v)
		}
	}
}

// TestFetch_ContextCancelledBeforeFlush cancels the caller's context before the
// flush window fires and verifies that Fetch returns ctx.Err().
func TestFetch_ContextCancelledBeforeFlush(t *testing.T) {
	// Use a long window so the flush won't fire naturally during the test.
	c := NewCoalescer[string, string](10*time.Second, slowFetcher(5*time.Second))
	startCtx, startCancel := context.WithCancel(context.Background())
	defer startCancel()
	go c.Start(startCtx)

	reqCtx, reqCancel := context.WithCancel(context.Background())

	type fetchOut struct {
		values map[string]string
		err    error
	}
	out := make(chan fetchOut, 1)
	go func() {
		v, e := c.Fetch(reqCtx, "x", "y")
		out <- fetchOut{v, e}
	}()

	// Give the Fetch goroutine time to enqueue before triggering flush.
	time.Sleep(time.Millisecond)

	// Cancel before the flush fires.
	reqCancel()

	// Manually trigger flush so the goroutines are spawned.
	go c.Flush(startCtx)

	result := <-out
	if !errors.Is(result.err, context.Canceled) {
		t.Errorf("want context.Canceled, got %v", result.err)
	}
}

// TestFetch_ContextCancelledDuringFetch cancels the caller's context while the
// fetcher is still running and verifies early delivery of ctx.Err().
func TestFetch_ContextCancelledDuringFetch(t *testing.T) {
	fetchStarted := make(chan struct{})
	blockFetch := make(chan struct{})

	fetcher := func(_ context.Context, keys []string) (map[string]string, error) {
		close(fetchStarted)
		<-blockFetch // block until we release it
		result := make(map[string]string)
		for _, k := range keys {
			result[k] = k
		}
		return result, nil
	}

	c := NewCoalescer[string, string](10*time.Second, fetcher)
	startCtx, startCancel := context.WithCancel(context.Background())
	defer startCancel()

	reqCtx, reqCancel := context.WithCancel(context.Background())

	type fetchOut struct {
		values map[string]string
		err    error
	}
	out := make(chan fetchOut, 1)
	go func() {
		v, e := c.Fetch(reqCtx, "p", "q")
		out <- fetchOut{v, e}
	}()

	// Give the Fetch goroutine time to enqueue before triggering flush.
	time.Sleep(time.Millisecond)

	go c.Flush(startCtx)

	// Wait until the fetcher has started, then cancel the request context.
	<-fetchStarted
	reqCancel()

	result := <-out
	if !errors.Is(result.err, context.Canceled) {
		t.Errorf("want context.Canceled, got %v", result.err)
	}

	// Unblock the fetcher so no goroutines are left hanging.
	close(blockFetch)
}

// TestFetch_FetcherError verifies that fetcher errors are propagated to callers.
func TestFetch_FetcherError(t *testing.T) {
	sentinel := errors.New("backend error")
	c := NewCoalescer[string, string](10*time.Millisecond, errorFetcher(sentinel))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Start(ctx)

	_, err := c.Fetch(context.Background(), "a")
	if !errors.Is(err, sentinel) {
		t.Errorf("want sentinel error, got %v", err)
	}
}

// TestFetch_NotFound verifies that keys absent from the fetcher result map are
// simply absent from the returned values map.
func TestFetch_NotFound(t *testing.T) {
	fetcher := func(_ context.Context, _ []string) (map[string]string, error) {
		return map[string]string{}, nil // return empty map — nothing found
	}
	c := NewCoalescer[string, string](10*time.Millisecond, fetcher)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Start(ctx)

	values, err := c.Fetch(context.Background(), "missing")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := values["missing"]; ok {
		t.Errorf("expected key \"missing\" to be absent from values map")
	}
}

// TestFetch_MultipleCallersSameKey verifies that multiple callers requesting the
// same key all receive the correct result.
func TestFetch_MultipleCallersSameKey(t *testing.T) {
	c := NewCoalescer[string, string](20*time.Millisecond, happyFetcher)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Start(ctx)

	const n = 5
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			values, err := c.Fetch(context.Background(), "shared")
			if err != nil {
				t.Errorf("caller %d: unexpected error: %v", idx, err)
				return
			}
			if v, ok := values["shared"]; !ok || v != "shared" {
				t.Errorf("caller %d: want \"shared\", got %q", idx, v)
			}
		}(i)
	}

	wg.Wait()
}

// TestFetch_StartContextCancelled verifies that cancelling the Start context
// drains pending requests with the Start context's error.
func TestFetch_StartContextCancelled(t *testing.T) {
	c := NewCoalescer[string, string](10*time.Second, slowFetcher(5*time.Second))
	startCtx, startCancel := context.WithCancel(context.Background())

	go c.Start(startCtx)

	type fetchOut struct {
		values map[string]string
		err    error
	}
	out := make(chan fetchOut, 1)
	go func() {
		v, e := c.Fetch(context.Background(), "z")
		out <- fetchOut{v, e}
	}()

	// Give Start a moment to register the request, then cancel it.
	time.Sleep(10 * time.Millisecond)
	startCancel()

	result := <-out
	if !errors.Is(result.err, context.Canceled) {
		t.Errorf("want context.Canceled, got %v", result.err)
	}
}
