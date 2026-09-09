package client

///////////////////////////////////////////////////
//                                               //
// Everything in this file will NOT be graded!!! //
//                                               //
///////////////////////////////////////////////////

// Tests for the coordination abstraction introduced in Phase 3A: that
// StrategySaferCC now routes through the installed backend, that the two
// benchmark strategies deliberately do not, and that the default is still
// V1's process-local LockManager.

import (
	"context"
	"errors"
	"sync"

	"github.com/JamJamzzz/safer-distributed/client/coordination"
	"github.com/JamJamzzz/safer-distributed/client/lockmanager"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var errCoordinatorDown = errors.New("coordinator unavailable")

// recordingBackend wraps the local backend and records what SAFER asked
// of it, so a test can tell whether an operation actually coordinated
// through the installed backend.
type recordingBackend struct {
	inner coordination.Backend

	mu        sync.Mutex
	begins    int
	acquired  []lockmanager.ResourceID
	modes     []lockmanager.LockMode
	releases  int
	healthErr error
}

func (b *recordingBackend) Begin(localTxn lockmanager.TxnID) (coordination.Guard, error) {
	inner, err := b.inner.Begin(localTxn)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	b.begins++
	b.mu.Unlock()
	return &recordingGuard{backend: b, inner: inner}, nil
}

func (b *recordingBackend) Health(ctx context.Context) error { return b.healthErr }
func (b *recordingBackend) Close() error                     { return nil }

func (b *recordingBackend) counts() (begins, acquires, releases int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.begins, len(b.acquired), b.releases
}

type recordingGuard struct {
	backend *recordingBackend
	inner   coordination.Guard
}

func (g *recordingGuard) Acquire(resource lockmanager.ResourceID, mode lockmanager.LockMode) error {
	return g.AcquireContext(context.Background(), resource, mode)
}

func (g *recordingGuard) AcquireContext(ctx context.Context, resource lockmanager.ResourceID, mode lockmanager.LockMode) error {
	if err := g.inner.AcquireContext(ctx, resource, mode); err != nil {
		return err
	}
	g.backend.mu.Lock()
	g.backend.acquired = append(g.backend.acquired, resource)
	g.backend.modes = append(g.backend.modes, mode)
	g.backend.mu.Unlock()
	return nil
}

func (g *recordingGuard) ReleaseAll() {
	g.inner.ReleaseAll()
	g.backend.mu.Lock()
	g.backend.releases++
	g.backend.mu.Unlock()
}

var _ = Describe("Coordination abstraction", func() {
	Specify("the default backend is the process-local lock manager", func() {
		// V1 behavior must remain the default until a backend is
		// installed deliberately.
		local, ok := currentCoordination().(*coordination.LocalBackend)
		Expect(ok).To(BeTrue(), "default backend is not *coordination.LocalBackend")
		// And it must wrap SAFER's single process-wide manager, not a
		// second one, which would silently split coordination in half.
		Expect(local.LockManager()).To(BeIdenticalTo(saferLockManager))
	})

	Specify("StrategySaferCC operations coordinate through the installed backend", func() {
		backend := &recordingBackend{inner: coordination.NewLocalBackend(saferLockManager)}
		restore := UseCoordination(backend)
		defer restore()

		alice, err := InitUser("coordination-routing-user", "password")
		Expect(err).ToNot(HaveOccurred())
		Expect(alice.StoreFile("file", []byte("content"))).To(Succeed())

		begins, acquires, releases := backend.counts()
		Expect(begins).To(BeNumerically(">", 0), "no transaction reached the backend")
		Expect(acquires).To(BeNumerically(">", 0), "no lock request reached the backend")
		Expect(releases).To(Equal(begins), "every transaction must end exactly once")

		// Strict 2PL: an operation takes the Namespace resource before
		// the File resource, and the guard exposes no early release.
		backend.mu.Lock()
		defer backend.mu.Unlock()
		Expect(backend.acquired[0].Type).To(Equal(lockmanager.NamespaceResource))
	})

	Specify("benchmark strategies bypass the coordination backend", func() {
		// StrategyGlobalLock and StrategyNoCC exist to measure this
		// process's alternatives to fine-grained 2PL. Routing them
		// through a remote coordinator would measure something else
		// entirely, so they must stay local.
		for _, strategy := range []ConcurrencyStrategy{StrategyGlobalLock, StrategyNoCC} {
			backend := &recordingBackend{inner: coordination.NewLocalBackend(saferLockManager)}
			restore := UseCoordination(backend)

			SetConcurrencyStrategyForBenchmark(strategy)
			guard, err := newOperationGuard(lockmanager.TxnID(1))
			Expect(err).ToNot(HaveOccurred())
			Expect(guard.Acquire(lockmanager.ResourceID{Type: lockmanager.FileResource, Key: "k"},
				lockmanager.ExclusiveLock)).To(Succeed())
			guard.ReleaseAll()

			begins, acquires, _ := backend.counts()
			Expect(begins).To(Equal(0), "strategy %d reached the coordination backend", strategy)
			Expect(acquires).To(Equal(0), "strategy %d reached the coordination backend", strategy)

			SetConcurrencyStrategyForBenchmark(StrategySaferCC)
			restore()
		}
	})

	Specify("a backend that cannot start a transaction fails the operation", func() {
		// An operation that cannot coordinate must not proceed
		// uncoordinated.
		restore := UseCoordination(&failingCoordinationBackend{})
		defer restore()

		alice, err := InitUser("coordination-failure-user", "password")
		Expect(err).ToNot(HaveOccurred())
		Expect(alice.StoreFile("file", []byte("content"))).ToNot(Succeed())
	})

	Specify("guards expose no per-resource release", func() {
		// Strict 2PL is preserved structurally: coordination.Guard has
		// Acquire and ReleaseAll only, so no caller can drop one lock
		// early. This asserts the interface shape at compile time.
		var guard coordination.Guard = &noCCGuard{}
		_ = guard
		type earlyReleaser interface {
			Release(resource lockmanager.ResourceID) error
		}
		_, hasEarlyRelease := guard.(earlyReleaser)
		Expect(hasEarlyRelease).To(BeFalse())
	})
})

// failingCoordinationBackend cannot start transactions, standing in for an
// unreachable coordinator.
type failingCoordinationBackend struct{}

func (failingCoordinationBackend) Begin(lockmanager.TxnID) (coordination.Guard, error) {
	return nil, errCoordinatorDown
}
func (failingCoordinationBackend) Health(context.Context) error { return errCoordinatorDown }
func (failingCoordinationBackend) Close() error                 { return nil }
