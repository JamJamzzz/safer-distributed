package grpccoord

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/JamJamzzz/safer-distributed/client/lockmanager"
)

// TestRemoteGuardAcquireContext_CancellationLeavesNoLockBehind exercises
// remoteGuard.AcquireContext directly -- not the raw RPC client the way
// TestCoordinatorCancelledAcquireIsNotGranted does -- because that is the
// method a SAFER operation actually calls (see client.StoreFileContext and
// friends). Before AcquireContext existed, remoteGuard.Acquire hard-coded
// context.Background() for the RPC, so a caller's cancellation never
// reached the coordinator at all; this test would have hung against that
// code (the cancelled call would never return, since nothing on the wire
// would have told the coordinator to drop the pending request).
func TestRemoteGuardAcquireContext_CancellationLeavesNoLockBehind(t *testing.T) {
	server, backend := startCoordinator(t)
	resource := fileResource("remoteguard-acquirecontext-cancel")

	holder, err := backend.Begin(0)
	if err != nil {
		t.Fatalf("Begin (holder): %v", err)
	}
	if err := holder.Acquire(resource, lockmanager.ExclusiveLock); err != nil {
		t.Fatalf("holder Acquire: %v", err)
	}

	waiter, err := backend.Begin(0)
	if err != nil {
		t.Fatalf("Begin (waiter): %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	waiterDone := make(chan error, 1)
	go func() {
		waiterDone <- waiter.AcquireContext(ctx, resource, lockmanager.ExclusiveLock)
	}()

	// Wait for the request to actually reach the coordinator's wait
	// queue, so cancellation below cancels a genuinely pending request
	// rather than racing the RPC's own dispatch.
	waitUntil(t, func() bool { return server.LockManager().QueueLen(resource) == 1 })
	cancel()

	select {
	case err := <-waiterDone:
		if status.Code(err) != codes.Canceled {
			t.Fatalf("cancelled AcquireContext: got %v, want a Canceled status", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("cancelled AcquireContext never returned")
	}

	// Removed from the queue...
	waitUntil(t, func() bool { return server.LockManager().QueueLen(resource) == 0 })

	// ...and holding nothing, even though its transaction still exists on
	// the coordinator (a cancelled Acquire is not the same as EndTransaction).
	waiter.ReleaseAll()

	// The decisive step: release the holder. A ghost lock granted to the
	// cancelled waiter would show up here as the resource staying held or
	// a later Acquire timing out.
	holder.ReleaseAll()
	waitUntil(t, func() bool { return server.LockManager().TotalHeldCount() == 0 })

	next, err := backend.Begin(0)
	if err != nil {
		t.Fatalf("Begin (next): %v", err)
	}
	granted := acquireAsync(next, resource, lockmanager.ExclusiveLock)
	expectGranted(t, granted, "later transaction after a cancelled AcquireContext")
	next.ReleaseAll()
}

// TestRemoteGuardAcquireContext_LegacyAcquireStillWorks pins Acquire's
// documented equivalence to AcquireContext(context.Background(), ...):
// existing callers that never adopted context propagation must keep
// working exactly as before.
func TestRemoteGuardAcquireContext_LegacyAcquireStillWorks(t *testing.T) {
	_, backend := startCoordinator(t)
	resource := fileResource("remoteguard-legacy-acquire")

	txn, err := backend.Begin(0)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := txn.Acquire(resource, lockmanager.ExclusiveLock); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	txn.ReleaseAll()
}
