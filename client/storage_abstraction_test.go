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
	restore := UseStorage(s)
	defer restore()
	fn()
}

var _ = Describe("Storage abstraction", func() {
	Specify("SAFER's default backend is the legacy userlib one", func() {
		// V1 behavior must remain the default until a backend is
		// explicitly substituted.
		Expect(currentStorage().Objects).To(BeAssignableToTypeOf(&storage.UserlibObjectStore{}))
		Expect(currentStorage().Keys).To(BeAssignableToTypeOf(&storage.UserlibKeyStore{}))
	})

	Specify("wrappers round-trip through whatever backend is installed", func() {
		userlib.DatastoreClear()
		defer userlib.DatastoreClear()

		id := uuid.New()
		value := []byte("envelope-bytes")
		Expect(datastoreSet(id, value)).To(Succeed())

		got, exists, err := datastoreGet(id)
		Expect(err).ToNot(HaveOccurred())
		Expect(exists).To(BeTrue())
		Expect(got).To(Equal(value))

		Expect(datastoreDelete(id)).To(Succeed())
		_, exists, err = datastoreGet(id)
		Expect(err).ToNot(HaveOccurred())
		Expect(exists).To(BeFalse())
	})

	Specify("a backend failure reaches the caller as an error", func() {
		// A failure must never be reported as a plain absence: SAFER
		// treats absence as a meaningful answer, so an outage would
		// otherwise read as "this file does not exist".
		withStorage(storage.Storage{Objects: failingObjectStore{}, Keys: failingKeyStore{}}, func() {
			_, _, err := datastoreGet(uuid.New())
			Expect(err).To(MatchError(errBackendDown))

			Expect(datastoreSet(uuid.New(), []byte("x"))).To(MatchError(errBackendDown))
			Expect(datastoreDelete(uuid.New())).To(MatchError(errBackendDown))

			_, _, err = keystoreGet("some-key-name")
			Expect(err).To(MatchError(errBackendDown))
		})
	})

	Specify("a failing backend fails SAFER operations instead of corrupting them", func() {
		// The end-to-end consequence: an unreachable backend must make
		// InitUser fail outright, not half-create an account.
		withStorage(storage.Storage{Objects: failingObjectStore{}, Keys: failingKeyStore{}}, func() {
			_, err := InitUser("storage-failure-user", "password")
			Expect(err).To(HaveOccurred())
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
