package client

import (
	"sync/atomic"

	"github.com/JamJamzzz/safer-distributed/client/lockmanager"
)

// FileMetadataSnapshot is a read-only view of a file's logical mutation
// bookkeeping.
//
// Version is SAFER's logical content version: every committed content
// mutation publishes exactly one increment, which makes it the oracle for
// "how many mutations actually landed" -- a lost update shows up as a
// version that advanced fewer times than there were successful writers.
type FileMetadataSnapshot struct {
	Version    uint64
	ChunkCount uint64
}

// ReadFileMetadata returns the caller's own view of a file's version
// bookkeeping.
//
// It exists as a correctness oracle for out-of-package tests, most
// importantly the cross-process ones, which cannot reach the internal
// resolveFile/loadMetadata path that the in-package tests use. Adding an
// exported accessor is preferable to weakening either the tests or the
// internals.
//
// It grants no new access: it authorizes exactly as LoadFile does, through
// the same namespace entry and access box, so a caller can only read
// metadata for a file it could already read. It leaks no content, no keys,
// and no information about files it cannot access.
//
// It participates in strict 2PL exactly as LoadFile does -- Shared on the
// namespace entry, then Shared on the file, released only when the
// transaction ends -- so a snapshot is never taken mid-mutation.
func (userdata *User) ReadFileMetadata(filename string) (FileMetadataSnapshot, error) {
	ctx := operationContext()

	txn, err := allocateTxnID()
	if err != nil {
		return FileMetadataSnapshot{}, err
	}

	guard, err := newOperationGuard(txn)
	if err != nil {
		return FileMetadataSnapshot{}, err
	}
	defer guard.ReleaseAll()

	nsResource, err := namespaceResourceID(userdata.Username, filename)
	if err != nil {
		return FileMetadataSnapshot{}, err
	}
	if err := guard.Acquire(nsResource, lockmanager.SharedLock); err != nil {
		return FileMetadataSnapshot{}, err
	}

	namespaceEntry, err := loadNamespaceEntry(ctx, userdata, filename)
	if err != nil {
		return FileMetadataSnapshot{}, err
	}

	fileResource := fileResourceID(namespaceEntry.FileID)
	if err := guard.Acquire(fileResource, lockmanager.SharedLock); err != nil {
		return FileMetadataSnapshot{}, err
	}

	accessBox, err := validateFileAccessUnderLock(ctx, namespaceEntry)
	if err != nil {
		return FileMetadataSnapshot{}, err
	}

	metadata, err := loadMetadata(ctx, accessBox)
	if err != nil {
		return FileMetadataSnapshot{}, err
	}

	return FileMetadataSnapshot{
		Version:    metadata.Version,
		ChunkCount: metadata.ChunkCount,
	}, nil
}

// ---------------------------------------------------------------------
// Test-only entry points.
//
// These exist so that out-of-process tests can build the deterministic
// schedules the in-package tests build with unexported hooks. A
// cross-process test cannot reach package internals, and the failure
// modes Phase 3C has to demonstrate -- a worker killed mid-operation, a
// worker that stalls and wakes up after losing its lock -- only exist
// across processes.
//
// Nothing in production calls any of these.

// SetTestPauseHook installs a hook fired at fixed points inside SAFER's
// lock-integrated operations, or clears it with nil.
//
// The tag identifies the call site and the resource, so a test can pause
// exactly one operation at exactly one point: "append:metadata-loaded"
// fires while the operation holds its locks and has read the state it is
// about to mutate, which is precisely the moment a stalled worker becomes
// dangerous.
func SetTestPauseHook(hook func(tag string)) {
	setConcurrencyTestHook(hook)
}

// disableFenceValidation, when set, marks every operation's context so
// that its storage transaction skips fencing validation.
//
// It exists for one test: the negative control that proves fencing is
// what stops a stale writer. A control that cannot demonstrate the bad
// commit happening without the check would prove nothing about the check.
var disableFenceValidation atomic.Bool

// DisableFenceValidationForTest turns fencing validation off for this
// process. Test-only, and deliberately loud in name: a process with this
// set has no stale-writer protection at all.
func DisableFenceValidationForTest(disabled bool) {
	disableFenceValidation.Store(disabled)
}
