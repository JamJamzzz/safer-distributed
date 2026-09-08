package client

///////////////////////////////////////////////////
//                                               //
// Everything in this file will NOT be graded!!! //
//                                               //
///////////////////////////////////////////////////

// Tests for the storage abstraction boundary introduced in SAFER
// Distributed Phase 1. The userlib backend never fails, so this is the
// first failure mode SAFER's storage wrappers have ever had to have an
// opinion about; these specs pin that opinion down before a backend that
// can actually fail (MongoDB, Phase 2) exists.

import (
	"context"
	"errors"

	userlib "github.com/cs161-staff/project2-userlib"
	"github.com/google/uuid"

	"github.com/cs161-staff/project2-starter-code/client/storage"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var errBackendDown = errors.New("backend unavailable")

// failingObjectStore fails every operation, standing in for a durable
// backend that is unreachable.
type failingObjectStore struct{}

func (failingObjectStore) Get(ctx context.Context, id uuid.UUID) ([]byte, bool, error) {
	return []byte("garbage"), true, errBackendDown
}
func (failingObjectStore) Put(ctx context.Context, id uuid.UUID, value []byte) error {
	return errBackendDown
}
func (failingObjectStore) Delete(ctx context.Context, id uuid.UUID) error { return errBackendDown }

type failingKeyStore struct{}

func (failingKeyStore) Get(ctx context.Context, name string) (userlib.PublicKeyType, bool, error) {
	return userlib.PublicKeyType{}, true, errBackendDown
}
func (failingKeyStore) Put(ctx context.Context, name string, key userlib.PublicKeyType) error {
	return errBackendDown
}

// withStorage swaps the process-wide backend for the duration of fn and
// restores it afterwards. Specs run serially, so this does not race with
// other specs.
func withStorage(s storage.Storage, fn func()) {
	previous := activeStorage
	activeStorage = s
	defer func() { activeStorage = previous }()
	fn()
}

var _ = Describe("Storage abstraction", func() {
	Specify("SAFER's default backend is the legacy userlib one", func() {
		// V1 behavior must remain the default until a backend is
		// explicitly substituted.
		Expect(activeStorage.Objects).To(BeAssignableToTypeOf(&storage.UserlibObjectStore{}))
		Expect(activeStorage.Keys).To(BeAssignableToTypeOf(&storage.UserlibKeyStore{}))
	})

	Specify("wrappers round-trip through whatever backend is installed", func() {
		userlib.DatastoreClear()
		defer userlib.DatastoreClear()

		id := uuid.New()
		value := []byte("envelope-bytes")
		datastoreSet(id, value)
		got, exists := datastoreGet(id)
		Expect(exists).To(BeTrue())
		Expect(got).To(Equal(value))

		datastoreDelete(id)
		_, exists = datastoreGet(id)
		Expect(exists).To(BeFalse())
	})

	Specify("a backend read failure reports absence, never bad bytes", func() {
		// The wrappers cannot return an error yet, so the one thing they
		// must not do is hand SAFER's crypto layer bytes the backend told
		// them not to trust.
		withStorage(storage.Storage{Objects: failingObjectStore{}, Keys: failingKeyStore{}}, func() {
			value, exists := datastoreGet(uuid.New())
			Expect(exists).To(BeFalse())
			Expect(value).To(BeNil())

			_, keyExists := keystoreGet("some-key-name")
			Expect(keyExists).To(BeFalse())
		})
	})

	Specify("a backend failure is recorded, not silently discarded", func() {
		withStorage(storage.Storage{Objects: failingObjectStore{}, Keys: failingKeyStore{}}, func() {
			datastoreSet(uuid.New(), []byte("x"))
			Expect(lastStorageFailure()).To(MatchError(errBackendDown))
			Expect(lastStorageFailure().Error()).To(ContainSubstring("datastoreSet"))

			datastoreDelete(uuid.New())
			Expect(lastStorageFailure().Error()).To(ContainSubstring("datastoreDelete"))

			datastoreGet(uuid.New())
			Expect(lastStorageFailure().Error()).To(ContainSubstring("datastoreGet"))
		})
	})

	Specify("keystore write errors still propagate to callers", func() {
		// keystoreSet already returned an error in V1, so this path loses
		// nothing: SAFER sees the backend failure directly.
		withStorage(storage.Storage{Objects: failingObjectStore{}, Keys: failingKeyStore{}}, func() {
			Expect(keystoreSet("some-key-name", userlib.PublicKeyType{})).To(MatchError(errBackendDown))
		})
	})
})
