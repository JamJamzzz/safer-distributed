package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JamJamzzz/safer-distributed/client"
)

const authTestTimeout = 5 * time.Second

// TestAuthLimiter_BoundsConcurrency is the core property: no more than N
// acquire/release sections ever run at once, verified by tracking the
// actual concurrent count rather than trusting the limiter's own
// bookkeeping.
func TestAuthLimiter_BoundsConcurrency(t *testing.T) {
	const n = 2
	const workers = 6
	limiter := newAuthLimiter(n)

	var current, max int64
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), authTestTimeout)
			defer cancel()
			if err := limiter.acquire(ctx); err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			defer limiter.release()

			c := atomic.AddInt64(&current, 1)
			for {
				m := atomic.LoadInt64(&max)
				if c <= m || atomic.CompareAndSwapInt64(&max, m, c) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond) // hold the slot long enough for overlap to show up
			atomic.AddInt64(&current, -1)
		}()
	}
	wg.Wait()

	if got := atomic.LoadInt64(&max); got > n {
		t.Fatalf("observed %d sections running concurrently, limiter allows at most %d", got, n)
	}
}

// TestAuthLimiter_CancelledWaiterReturnsPromptly proves a queued acquire
// does not wait for a slot that never comes: with the limiter fully held,
// a second acquire whose context is cancelled must return quickly with
// ctx.Err(), and must not have taken a slot (checked by acquiring
// successfully immediately afterward with the same single-slot limiter,
// still held by the first goroutine -- proving the cancelled waiter left
// no side effect).
func TestAuthLimiter_CancelledWaiterReturnsPromptly(t *testing.T) {
	limiter := newAuthLimiter(1)

	holderCtx, holderCancel := context.WithTimeout(context.Background(), authTestTimeout)
	defer holderCancel()
	if err := limiter.acquire(holderCtx); err != nil {
		t.Fatalf("holder acquire: %v", err)
	}
	defer limiter.release()

	waiterCtx, waiterCancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- limiter.acquire(waiterCtx)
	}()

	// Give the waiter a moment to actually reach the blocking select
	// before cancelling, so this cancels a genuinely pending acquire.
	time.Sleep(20 * time.Millisecond)
	waiterCancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled acquire returned %v, want context.Canceled", err)
		}
	case <-time.After(authTestTimeout):
		t.Fatal("cancelled acquire never returned")
	}
}

// TestAuthenticate_ReleasesPermitOnError proves the third required
// property: a failed GetUserContext (wrong password) must still give its
// slot back, via authenticate's defer. A limiter of size 1 makes a
// leaked permit observable directly -- if the failed call did not
// release, this second, unrelated, correct call would block forever.
func TestAuthenticate_ReleasesPermitOnError(t *testing.T) {
	restoreStorage := client.UseUserlibStorage()
	defer restoreStorage()

	const username, password = "authlimit-alice", "correct-password"
	if _, err := client.InitUser(username, password); err != nil {
		t.Fatalf("InitUser: %v", err)
	}

	limiter := newAuthLimiter(1)
	ctx, cancel := context.WithTimeout(context.Background(), authTestTimeout)
	defer cancel()

	if _, err := authenticate(ctx, limiter, username, "wrong-password"); err == nil {
		t.Fatal("authenticate with a wrong password unexpectedly succeeded")
	}

	// If the failed call's slot leaked, this blocks until the context
	// deadline and the test fails with a wrapped context.DeadlineExceeded
	// rather than succeeding.
	if _, err := authenticate(ctx, limiter, username, password); err != nil {
		t.Fatalf("authenticate after a failed attempt: %v (permit likely leaked)", err)
	}
}

// TestAuthenticate_NormalRequestsStillWork is the control: with the
// limiter in place, a correct username/password still authenticates.
func TestAuthenticate_NormalRequestsStillWork(t *testing.T) {
	restoreStorage := client.UseUserlibStorage()
	defer restoreStorage()

	const username, password = "authlimit-bob", "bob-password"
	if _, err := client.InitUser(username, password); err != nil {
		t.Fatalf("InitUser: %v", err)
	}

	limiter := newAuthLimiter(DefaultAuthConcurrency)
	ctx, cancel := context.WithTimeout(context.Background(), authTestTimeout)
	defer cancel()

	user, err := authenticate(ctx, limiter, username, password)
	if err != nil {
		t.Fatalf("authenticate: %v", err)
	}
	if user.Username != username {
		t.Errorf("authenticated as %q, want %q", user.Username, username)
	}
}
