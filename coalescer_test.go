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

// collectResult reads the single result from the result channel.
func collectResult(ch <-chan Result[string, string]) Result[string, string] {
	return <-ch
}

// TestFetch_HappyPath verifies that a normal fetch delivers the expected values.
func TestFetch_HappyPath(t *testing.T) {
	c := NewCoalescer[string, string](10*time.Millisecond, happyFetcher)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go c.Start(ctx)

	ch := c.Fetch(context.Background(), "a", "b", "c")
	result := collectResult(ch)

	for _, key := range []string{"a", "b", "c"} {
		if err, ok := result.Errors[key]; ok {
			t.Errorf("unexpected error for key %q: %v", key, err)
		}
		if v, ok := result.Values[key]; !ok || v != key {
			t.Errorf("key %q: want value %q, got %q", key, key, v)
		}
	}
}

// TestFetch_ContextCancelledBeforeFlush cancels the caller's context before the
// flush window fires and verifies that the result channel returns ctx.Err().
func TestFetch_ContextCancelledBeforeFlush(t *testing.T) {
	// Use a long window so the flush won't fire naturally during the test.
	c := NewCoalescer[string, string](10*time.Second, slowFetcher(5*time.Second))
	startCtx, startCancel := context.WithCancel(context.Background())
	defer startCancel()
	go c.Start(startCtx)

	reqCtx, reqCancel := context.WithCancel(context.Background())
	ch := c.Fetch(reqCtx, "x", "y")

	// Cancel before the flush fires.
	reqCancel()

	// Manually trigger flush so the goroutines are spawned.
	go c.Flush(startCtx)

	result := collectResult(ch)

	for _, key := range []string{"x", "y"} {
		if !errors.Is(result.Errors[key], context.Canceled) {
			t.Errorf("key %q: want context.Canceled, got %v", key, result.Errors[key])
		}
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
	ch := c.Fetch(reqCtx, "p", "q")

	go c.Flush(startCtx)

	// Wait until the fetcher has started, then cancel the request context.
	<-fetchStarted
	reqCancel()

	result := collectResult(ch)

	for _, key := range []string{"p", "q"} {
		if !errors.Is(result.Errors[key], context.Canceled) {
			t.Errorf("key %q: want context.Canceled, got %v", key, result.Errors[key])
		}
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

	ch := c.Fetch(context.Background(), "a")
	result := collectResult(ch)

	if !errors.Is(result.Errors["a"], sentinel) {
		t.Errorf("want sentinel error, got %v", result.Errors["a"])
	}
}

// TestFetch_NotFound verifies that keys absent from the fetcher result map get ErrNotFound.
func TestFetch_NotFound(t *testing.T) {
	fetcher := func(_ context.Context, _ []string) (map[string]string, error) {
		return map[string]string{}, nil // return empty map — nothing found
	}
	c := NewCoalescer[string, string](10*time.Millisecond, fetcher)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Start(ctx)

	ch := c.Fetch(context.Background(), "missing")
	result := collectResult(ch)

	if !errors.Is(result.Errors["missing"], ErrNotFound) {
		t.Errorf("want ErrNotFound, got %v", result.Errors["missing"])
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
	channels := make([]<-chan Result[string, string], n)
	for i := range n {
		channels[i] = c.Fetch(context.Background(), "shared")
	}

	var wg sync.WaitGroup
	for i, ch := range channels {
		wg.Add(1)
		go func(idx int, ch <-chan Result[string, string]) {
			defer wg.Done()
			result := collectResult(ch)
			if err, ok := result.Errors["shared"]; ok {
				t.Errorf("caller %d: unexpected error: %v", idx, err)
			}
			if v, ok := result.Values["shared"]; !ok || v != "shared" {
				t.Errorf("caller %d: want \"shared\", got %q", idx, v)
			}
		}(i, ch)
	}

	wg.Wait()
}

// TestFetch_StartContextCancelled verifies that cancelling the Start context
// drains pending requests with the Start context's error.
func TestFetch_StartContextCancelled(t *testing.T) {
	c := NewCoalescer[string, string](10*time.Second, slowFetcher(5*time.Second))
	startCtx, startCancel := context.WithCancel(context.Background())

	go c.Start(startCtx)

	ch := c.Fetch(context.Background(), "z")

	// Give Start a moment to register the request, then cancel it.
	time.Sleep(10 * time.Millisecond)
	startCancel()

	result := collectResult(ch)
	if !errors.Is(result.Errors["z"], context.Canceled) {
		t.Errorf("want context.Canceled, got %v", result.Errors["z"])
	}
}
