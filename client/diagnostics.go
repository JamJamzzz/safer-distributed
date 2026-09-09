package client

import (
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

	namespaceEntry, err := loadNamespaceEntry(userdata, filename)
	if err != nil {
		return FileMetadataSnapshot{}, err
	}

	fileResource := fileResourceID(namespaceEntry.FileID)
	if err := guard.Acquire(fileResource, lockmanager.SharedLock); err != nil {
		return FileMetadataSnapshot{}, err
	}

	accessBox, err := validateFileAccessUnderLock(namespaceEntry)
	if err != nil {
		return FileMetadataSnapshot{}, err
	}

	metadata, err := loadMetadata(accessBox)
	if err != nil {
		return FileMetadataSnapshot{}, err
	}

	return FileMetadataSnapshot{
		Version:    metadata.Version,
		ChunkCount: metadata.ChunkCount,
	}, nil
}
