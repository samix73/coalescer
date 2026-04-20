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

func collectResults(results FetchResult[string, string]) map[string]Result[string, string] {
	out := make(map[string]Result[string, string])
	for _, r := range results {
		out[r.Key] = r
	}
	return out
}

func awaitFetch(t *testing.T, ch <-chan FetchResult[string, string]) FetchResult[string, string] {
	t.Helper()

	select {
	case results := <-ch:
		return results
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for Fetch result")
		return nil
	}
}

func waitForPending(t *testing.T, c *Coalescer[string, string], want int) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		pending := len(c.pending)
		c.mu.Unlock()

		if pending >= want {
			return
		}

		time.Sleep(time.Millisecond)
	}

	t.Fatalf("timed out waiting for pending requests: want >= %d", want)
}

// TestFetch_HappyPath verifies that a normal fetch delivers the expected values.
func TestFetch_HappyPath(t *testing.T) {
	c := NewCoalescer[string, string](10*time.Millisecond, happyFetcher)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go c.Start(ctx)

	resultCh := make(chan FetchResult[string, string], 1)
	go func() {
		resultCh <- c.Fetch(context.Background(), "a", "b", "c")
	}()

	results := collectResults(awaitFetch(t, resultCh))

	for _, key := range []string{"a", "b", "c"} {
		r, ok := results[key]
		if !ok {
			t.Errorf("missing result for key %q", key)
			continue
		}
		if r.Err != nil {
			t.Errorf("unexpected error for key %q: %v", key, r.Err)
		}
		if r.Value != key {
			t.Errorf("key %q: want value %q, got %q", key, key, r.Value)
		}
	}
}

// TestFetch_ContextCancelledBeforeFlush cancels the caller's context before the
// flush window fires and verifies Fetch returns immediately with ctx.Err().
func TestFetch_ContextCancelledBeforeFlush(t *testing.T) {
	// Use a long window so the flush won't fire naturally during the test.
	c := NewCoalescer[string, string](10*time.Second, slowFetcher(5*time.Second))
	startCtx, startCancel := context.WithCancel(context.Background())
	defer startCancel()
	go c.Start(startCtx)

	reqCtx, reqCancel := context.WithCancel(context.Background())
	resultCh := make(chan FetchResult[string, string], 1)
	go func() {
		resultCh <- c.Fetch(reqCtx, "x", "y")
	}()
	waitForPending(t, c, 1)

	// Cancel before the flush fires.
	reqCancel()

	results := collectResults(awaitFetch(t, resultCh))

	for _, key := range []string{"x", "y"} {
		r, ok := results[key]
		if !ok {
			t.Errorf("missing result for key %q", key)
			continue
		}
		if !errors.Is(r.Err, context.Canceled) {
			t.Errorf("key %q: want context.Canceled, got %v", key, r.Err)
		}
	}
}

// TestFlush_SkipsCancelledRequestsBeforeFetch verifies Flush does not pass keys
// from already-cancelled requests to the fetcher.
func TestFlush_SkipsCancelledRequestsBeforeFetch(t *testing.T) {
	var (
		mu       sync.Mutex
		gotCalls [][]string
	)

	fetcher := func(_ context.Context, keys []string) (map[string]string, error) {
		mu.Lock()
		gotCalls = append(gotCalls, append([]string(nil), keys...))
		mu.Unlock()

		result := make(map[string]string, len(keys))
		for _, k := range keys {
			result[k] = k
		}
		return result, nil
	}

	c := NewCoalescer[string, string](10*time.Second, fetcher)
	startCtx, startCancel := context.WithCancel(context.Background())
	defer startCancel()

	cancelledCtx, cancelCancelledCtx := context.WithCancel(context.Background())
	cancelledResultCh := make(chan FetchResult[string, string], 1)
	go func() {
		cancelledResultCh <- c.Fetch(cancelledCtx, "cancelled-key")
	}()
	waitForPending(t, c, 1)
	cancelCancelledCtx()

	activeResultCh := make(chan FetchResult[string, string], 1)
	go func() {
		activeResultCh <- c.Fetch(context.Background(), "active-key")
	}()
	waitForPending(t, c, 2)

	c.Flush(startCtx)

	cancelledResults := collectResults(awaitFetch(t, cancelledResultCh))
	activeResults := collectResults(awaitFetch(t, activeResultCh))

	cancelled, ok := cancelledResults["cancelled-key"]
	if !ok {
		t.Fatal("missing result for key \"cancelled-key\"")
	}
	if !errors.Is(cancelled.Err, context.Canceled) {
		t.Errorf("want context.Canceled for cancelled-key, got %v", cancelled.Err)
	}

	active, ok := activeResults["active-key"]
	if !ok {
		t.Fatal("missing result for key \"active-key\"")
	}
	if active.Err != nil {
		t.Errorf("unexpected error for active-key: %v", active.Err)
	}
	if active.Value != "active-key" {
		t.Errorf("want active-key value, got %q", active.Value)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(gotCalls) != 1 {
		t.Fatalf("fetcher call count: want 1, got %d", len(gotCalls))
	}
	if len(gotCalls[0]) != 1 || gotCalls[0][0] != "active-key" {
		t.Fatalf("fetcher keys: want [active-key], got %v", gotCalls[0])
	}
}

// TestFlush_AllRequestsCancelledSkipsFetch verifies Flush avoids calling the
// fetcher when all pending requests are already cancelled.
func TestFlush_AllRequestsCancelledSkipsFetch(t *testing.T) {
	callCount := 0
	fetcher := func(_ context.Context, keys []string) (map[string]string, error) {
		callCount++
		t.Fatalf("fetcher should not be called, got keys: %v", keys)
		return nil, nil
	}

	c := NewCoalescer[string, string](10*time.Second, fetcher)
	startCtx, startCancel := context.WithCancel(context.Background())
	defer startCancel()

	reqCtx, reqCancel := context.WithCancel(context.Background())
	resultCh := make(chan FetchResult[string, string], 1)
	go func() {
		resultCh <- c.Fetch(reqCtx, "only-cancelled")
	}()
	waitForPending(t, c, 1)
	reqCancel()

	c.Flush(startCtx)

	results := collectResults(awaitFetch(t, resultCh))
	r, ok := results["only-cancelled"]
	if !ok {
		t.Fatal("missing result for key \"only-cancelled\"")
	}
	if !errors.Is(r.Err, context.Canceled) {
		t.Errorf("want context.Canceled, got %v", r.Err)
	}
	if callCount != 0 {
		t.Fatalf("fetcher call count: want 0, got %d", callCount)
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
	resultCh := make(chan FetchResult[string, string], 1)
	go func() {
		resultCh <- c.Fetch(reqCtx, "p", "q")
	}()
	waitForPending(t, c, 1)

	go c.Flush(startCtx)

	// Wait until the fetcher has started, then cancel the request context.
	<-fetchStarted
	reqCancel()

	results := collectResults(awaitFetch(t, resultCh))

	for _, key := range []string{"p", "q"} {
		r, ok := results[key]
		if !ok {
			t.Errorf("missing result for key %q", key)
			continue
		}
		if !errors.Is(r.Err, context.Canceled) {
			t.Errorf("key %q: want context.Canceled, got %v", key, r.Err)
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

	resultCh := make(chan FetchResult[string, string], 1)
	go func() {
		resultCh <- c.Fetch(context.Background(), "a")
	}()
	results := collectResults(awaitFetch(t, resultCh))

	r, ok := results["a"]
	if !ok {
		t.Fatal("missing result for key \"a\"")
	}
	if !errors.Is(r.Err, sentinel) {
		t.Errorf("want sentinel error, got %v", r.Err)
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

	resultCh := make(chan FetchResult[string, string], 1)
	go func() {
		resultCh <- c.Fetch(context.Background(), "missing")
	}()
	results := collectResults(awaitFetch(t, resultCh))

	r, ok := results["missing"]
	if !ok {
		t.Fatal("missing result for key \"missing\"")
	}
	if !errors.Is(r.Err, ErrNotFound) {
		t.Errorf("want ErrNotFound, got %v", r.Err)
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
	results := make([]<-chan FetchResult[string, string], n)
	for i := range n {
		resultCh := make(chan FetchResult[string, string], 1)
		results[i] = resultCh
		go func(ch chan<- FetchResult[string, string]) {
			ch <- c.Fetch(context.Background(), "shared")
		}(resultCh)
	}

	var wg sync.WaitGroup
	for i, ch := range results {
		wg.Add(1)
		go func(idx int, ch <-chan FetchResult[string, string]) {
			defer wg.Done()
			res := collectResults(awaitFetch(t, ch))
			r, ok := res["shared"]
			if !ok {
				t.Errorf("caller %d: missing result for key \"shared\"", idx)
				return
			}
			if r.Err != nil {
				t.Errorf("caller %d: unexpected error: %v", idx, r.Err)
			}
			if r.Value != "shared" {
				t.Errorf("caller %d: want \"shared\", got %q", idx, r.Value)
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

	resultCh := make(chan FetchResult[string, string], 1)
	go func() {
		resultCh <- c.Fetch(context.Background(), "z")
	}()

	// Give Start a moment to register the request, then cancel it.
	time.Sleep(10 * time.Millisecond)
	startCancel()

	results := collectResults(awaitFetch(t, resultCh))
	r, ok := results["z"]
	if !ok {
		t.Fatal("missing result for key \"z\"")
	}
	if !errors.Is(r.Err, context.Canceled) {
		t.Errorf("want context.Canceled, got %v", r.Err)
	}
}
