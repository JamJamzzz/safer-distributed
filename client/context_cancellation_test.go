package client

///////////////////////////////////////////////////
//                                               //
// Everything in this file will NOT be graded!!! //
//                                               //
///////////////////////////////////////////////////

// Tests that a caller-supplied context, threaded through the *Context
// entry points (StoreFileContext, AppendToFileContext, LoadFileContext,
// and the guard.AcquireContext call they now make instead of the
// context-free Acquire), actually reaches lock acquisition -- not just at
// the lockmanager level, which client/lockmanager/lockmanager_context_test.go
// already covers exhaustively, but at the SAFER-operation level a real
// caller (a gRPC handler, in cmd/worker) actually drives.
//
// These are white-box (package client) so they can inspect the process-
// wide lock manager's wait-queue length directly, and use the existing
// concurrencyTestHook to force a deterministic schedule instead of racing
// a sleep.

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JamJamzzz/safer-distributed/client/lockmanager"
)

// waitUntilTB polls cond until it is true or timeout elapses, failing the
// test otherwise. Used instead of a fixed sleep so these tests do not
// become flaky under a slow CI host, matching the style of
// client/lockmanager/lockmanager_context_test.go's own waitUntil.
func waitUntilTB(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	if !cond() {
		t.Fatal("condition was not met before the timeout")
	}
}

// fileResourceFor resolves filename's current lockmanager.ResourceID, the
// same one AppendToFileContext/StoreFileContext/LoadFileContext acquire
// File-mode locks on, so a test can inspect the wait queue for exactly the
// resource it expects contention on.
func fileResourceFor(t *testing.T, userdata *User, filename string) lockmanager.ResourceID {
	t.Helper()
	entry, err := loadNamespaceEntry(context.Background(), userdata, filename)
	if err != nil {
		t.Fatalf("loadNamespaceEntry: %v", err)
	}
	return fileResourceID(entry.FileID)
}

// TestAppendToFileContext_CancellationLeavesNoLockBehind is this
// package's version of Phase 4.5's required evidence: cancelling an
// operation blocked waiting for a lock terminates it, its pending
// acquisition is removed, no lock is leaked, and a later caller can
// still acquire and proceed.
func TestAppendToFileContext_CancellationLeavesNoLockBehind(t *testing.T) {
	restore := UseLocalCoordination()
	defer restore()
	setConcurrencyTestHook(nil)
	defer setConcurrencyTestHook(nil)

	alice, err := InitUser("ctxcancel-alice", "password")
	if err != nil {
		t.Fatalf("InitUser: %v", err)
	}
	if err := alice.StoreFile("shared.txt", []byte("base")); err != nil {
		t.Fatalf("StoreFile: %v", err)
	}
	fileResource := fileResourceFor(t, alice, "shared.txt")

	// Pause the FIRST append after it has already acquired File X (the
	// "append:metadata-loaded" hook fires strictly after that acquire --
	// see AppendToFileContext), so it genuinely holds the lock the second
	// append below contends for, rather than racing a goroutine that has
	// not gotten there yet.
	holding := make(chan struct{})
	release := make(chan struct{})
	var pauseOnce sync.Once
	setConcurrencyTestHook(func(tag string) {
		if tag == "append:metadata-loaded:shared.txt" {
			pauseOnce.Do(func() {
				close(holding)
				<-release
			})
		}
	})

	firstDone := make(chan error, 1)
	go func() { firstDone <- alice.AppendToFile("shared.txt", []byte("-first")) }()
	<-holding

	ctx, cancel := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() { secondDone <- alice.AppendToFileContext(ctx, "shared.txt", []byte("-second")) }()

	// Wait until the second append is really queued for File X (not the
	// namespace S lock, which the first append does not hold exclusively
	// and so never blocks a second shared request).
	waitUntilTB(t, 2*time.Second, func() bool { return saferLockManager.QueueLen(fileResource) == 1 })

	cancel()

	select {
	case err := <-secondDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled AppendToFileContext returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled AppendToFileContext did not return")
	}

	// Removed from the queue...
	waitUntilTB(t, 2*time.Second, func() bool { return saferLockManager.QueueLen(fileResource) == 0 })

	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first AppendToFile: %v", err)
	}

	// The decisive step: a later caller must be able to acquire and
	// proceed. A leaked lock from the cancelled request would wedge this.
	thirdDone := make(chan error, 1)
	go func() { thirdDone <- alice.AppendToFile("shared.txt", []byte("-third")) }()
	select {
	case err := <-thirdDone:
		if err != nil {
			t.Fatalf("third AppendToFile after cancellation: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a later caller could not acquire the lock after the cancelled request; a lock leaked")
	}

	content, err := alice.LoadFile("shared.txt")
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if strings.Contains(string(content), "-second") {
		t.Fatalf("cancelled append's content is present in the file: %q", content)
	}
	if !strings.Contains(string(content), "-first") || !strings.Contains(string(content), "-third") {
		t.Fatalf("expected both surviving appends' content in %q", content)
	}
}

// TestAppendToFileContext_AlreadyCancelledNeverAcquires covers a request
// that is cancelled before it is even sent: it must return promptly with
// context.Canceled and take no lock at all, rather than being granted
// once and then having nothing to release it (guard.ReleaseAll still runs
// via defer inside AppendToFileContext regardless, but the grant should
// never have happened in the first place).
func TestAppendToFileContext_AlreadyCancelledNeverAcquires(t *testing.T) {
	restore := UseLocalCoordination()
	defer restore()

	alice, err := InitUser("ctxcancel-bob", "password")
	if err != nil {
		t.Fatalf("InitUser: %v", err)
	}
	if err := alice.StoreFile("shared2.txt", []byte("base")); err != nil {
		t.Fatalf("StoreFile: %v", err)
	}
	fileResource := fileResourceFor(t, alice, "shared2.txt")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = alice.AppendToFileContext(ctx, "shared2.txt", []byte("-never"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("AppendToFileContext with an already-cancelled context returned %v, want context.Canceled", err)
	}
	if got := saferLockManager.QueueLen(fileResource); got != 0 {
		t.Fatalf("QueueLen after an already-cancelled AppendToFileContext = %d, want 0", got)
	}

	// The resource must still be immediately usable.
	if err := alice.AppendToFile("shared2.txt", []byte("-ok")); err != nil {
		t.Fatalf("AppendToFile after an already-cancelled AppendToFileContext: %v", err)
	}
}
