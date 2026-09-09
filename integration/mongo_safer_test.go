// Package integration runs the real, public SAFER API against the
// MongoDB storage backend.
//
// It lives outside the client package on purpose: the MongoDB driver
// stays a dependency of the backend and this test, never of SAFER's core
// client package.
//
// Every test skips cleanly when SAFER_MONGO_URI is unset or MongoDB is
// unreachable, so the repository's suite still runs on a machine with no
// MongoDB. To run these:
//
//	docker run -d -p 27017:27017 --name safer-mongo mongo:7
//	SAFER_MONGO_URI=mongodb://localhost:27017 go test ./integration/
//
// Scope note: these tests demonstrate that SAFER's semantics survive on a
// durable shared backend, and that data outlives the process state. They
// are NOT evidence of distributed correctness. SAFER's locking is still
// process-local, so two SAFER processes sharing one MongoDB are not yet
// safely serialized; that requires the Phase 3 lock coordinator.
package integration

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/JamJamzzz/safer-distributed/client"
	"github.com/JamJamzzz/safer-distributed/client/storage/mongostore"
)

// requireMongoSAFER points the SAFER client at a private MongoDB database
// for the duration of one test.
func requireMongoSAFER(t *testing.T) {
	t.Helper()

	cfg, configured, err := mongostore.ConfigFromEnv()
	if err != nil {
		t.Fatalf("bad MongoDB configuration: %v", err)
	}
	if !configured {
		t.Skipf("skipping: %s is not set", mongostore.EnvURI)
	}
	cfg.Database = fmt.Sprintf("safer_e2e_%d", time.Now().UnixNano())

	ctx := context.Background()
	store, err := mongostore.Open(ctx, cfg)
	if err != nil {
		t.Skipf("skipping: MongoDB is unreachable: %v", err)
	}

	restore := client.UseStorage(store.Storage())
	t.Cleanup(func() {
		restore()
		if err := store.DropDatabase(context.Background()); err != nil {
			t.Errorf("dropping test database: %v", err)
		}
		if err := store.Close(context.Background()); err != nil {
			t.Errorf("closing store: %v", err)
		}
	})
}

// TestSAFEROnMongoStoreLoadAppend runs SAFER's core file lifecycle against
// MongoDB. Ciphertext lives in MongoDB; every cryptographic and
// authorization decision still happens in SAFER.
func TestSAFEROnMongoStoreLoadAppend(t *testing.T) {
	requireMongoSAFER(t)

	alice, err := client.InitUser("alice", "alice-password")
	if err != nil {
		t.Fatalf("InitUser: %v", err)
	}

	content := []byte("first")
	if err := alice.StoreFile("notes", content); err != nil {
		t.Fatalf("StoreFile: %v", err)
	}
	got, err := alice.LoadFile("notes")
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("LoadFile: got %q, want %q", got, content)
	}

	if err := alice.AppendToFile("notes", []byte("-second")); err != nil {
		t.Fatalf("AppendToFile: %v", err)
	}
	got, err = alice.LoadFile("notes")
	if err != nil {
		t.Fatalf("LoadFile after append: %v", err)
	}
	if want := []byte("first-second"); !bytes.Equal(got, want) {
		t.Fatalf("after append: got %q, want %q", got, want)
	}

	// Overwrite must replace, not append.
	if err := alice.StoreFile("notes", []byte("replaced")); err != nil {
		t.Fatalf("overwriting StoreFile: %v", err)
	}
	if got, err = alice.LoadFile("notes"); err != nil || !bytes.Equal(got, []byte("replaced")) {
		t.Fatalf("after overwrite: got %q err=%v, want %q", got, err, "replaced")
	}
}

// TestSAFEROnMongoAuthSemantics checks that the security-relevant answers
// are unchanged by the backend swap.
func TestSAFEROnMongoAuthSemantics(t *testing.T) {
	requireMongoSAFER(t)

	alice, err := client.InitUser("alice", "alice-password")
	if err != nil {
		t.Fatalf("InitUser: %v", err)
	}
	if _, err := client.InitUser("alice", "another-password"); err == nil {
		t.Fatal("InitUser succeeded for a taken username, want an error")
	}
	if _, err := client.GetUser("alice", "wrong-password"); err == nil {
		t.Fatal("GetUser succeeded with the wrong password, want an error")
	}
	if _, err := client.GetUser("nobody", "password"); err == nil {
		t.Fatal("GetUser succeeded for an unknown user, want an error")
	}

	if err := alice.StoreFile("secret", []byte("classified")); err != nil {
		t.Fatalf("StoreFile: %v", err)
	}
	bob, err := client.InitUser("bob", "bob-password")
	if err != nil {
		t.Fatalf("InitUser bob: %v", err)
	}
	// Bob has no capability for Alice's file, and filenames are per-user.
	if _, err := bob.LoadFile("secret"); err == nil {
		t.Fatal("bob loaded alice's file without an invitation")
	}
}

// TestSAFEROnMongoSharingAndRevocation exercises the capability path end
// to end on the durable backend.
func TestSAFEROnMongoSharingAndRevocation(t *testing.T) {
	requireMongoSAFER(t)

	alice, err := client.InitUser("alice", "alice-password")
	if err != nil {
		t.Fatalf("InitUser alice: %v", err)
	}
	bob, err := client.InitUser("bob", "bob-password")
	if err != nil {
		t.Fatalf("InitUser bob: %v", err)
	}

	content := []byte("shared content")
	if err := alice.StoreFile("shared", content); err != nil {
		t.Fatalf("StoreFile: %v", err)
	}

	invite, err := alice.CreateInvitation("shared", "bob")
	if err != nil {
		t.Fatalf("CreateInvitation: %v", err)
	}
	if err := bob.AcceptInvitation("alice", invite, "from-alice"); err != nil {
		t.Fatalf("AcceptInvitation: %v", err)
	}

	got, err := bob.LoadFile("from-alice")
	if err != nil {
		t.Fatalf("bob LoadFile: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("bob read %q, want %q", got, content)
	}

	// Writes are shared through the capability.
	if err := bob.AppendToFile("from-alice", []byte("+bob")); err != nil {
		t.Fatalf("bob AppendToFile: %v", err)
	}
	if got, err = alice.LoadFile("shared"); err != nil || !bytes.Equal(got, []byte("shared content+bob")) {
		t.Fatalf("alice after bob's append: got %q err=%v", got, err)
	}

	if err := alice.RevokeAccess("shared", "bob"); err != nil {
		t.Fatalf("RevokeAccess: %v", err)
	}
	if _, err := bob.LoadFile("from-alice"); err == nil {
		t.Fatal("bob still read the file after revocation")
	}
	if err := bob.AppendToFile("from-alice", []byte("+bob-again")); err == nil {
		t.Fatal("bob still appended after revocation")
	}
	// Alice keeps her data.
	if got, err = alice.LoadFile("shared"); err != nil || !bytes.Equal(got, []byte("shared content+bob")) {
		t.Fatalf("alice after revocation: got %q err=%v", got, err)
	}
}

// TestSAFEROnMongoSurvivesClientRestart is the durability claim stated in
// SAFER's own terms: a user and their file, created through one SAFER
// client and one connection pool, are readable through a completely
// separate client afterwards.
//
// The in-memory userlib backend cannot do this. Note the scope: this shows
// data outlives the client and its connection, which is what a worker
// restart looks like from storage's point of view. It is not a crash-
// consistency or fault-tolerance claim.
func TestSAFEROnMongoSurvivesClientRestart(t *testing.T) {
	cfg, configured, err := mongostore.ConfigFromEnv()
	if err != nil {
		t.Fatalf("bad MongoDB configuration: %v", err)
	}
	if !configured {
		t.Skipf("skipping: %s is not set", mongostore.EnvURI)
	}
	cfg.Database = fmt.Sprintf("safer_e2e_restart_%d", time.Now().UnixNano())
	ctx := context.Background()

	// First "worker": create the user and store a file, then shut down
	// its storage entirely.
	first, err := mongostore.Open(ctx, cfg)
	if err != nil {
		t.Skipf("skipping: MongoDB is unreachable: %v", err)
	}
	restore := client.UseStorage(first.Storage())

	alice, err := client.InitUser("alice", "alice-password")
	if err != nil {
		restore()
		t.Fatalf("InitUser: %v", err)
	}
	content := []byte("must survive the restart")
	if err := alice.StoreFile("durable", content); err != nil {
		restore()
		t.Fatalf("StoreFile: %v", err)
	}
	restore()
	if err := first.Close(ctx); err != nil {
		t.Fatalf("closing first store: %v", err)
	}

	// Second "worker": a fresh connection pool and a fresh User struct,
	// with no in-process state carried over.
	second, err := mongostore.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("reopening: %v", err)
	}
	restore = client.UseStorage(second.Storage())
	t.Cleanup(func() {
		restore()
		_ = second.DropDatabase(context.Background())
		_ = second.Close(context.Background())
	})

	revived, err := client.GetUser("alice", "alice-password")
	if err != nil {
		t.Fatalf("GetUser after restart: %v", err)
	}
	got, err := revived.LoadFile("durable")
	if err != nil {
		t.Fatalf("LoadFile after restart: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("after restart: got %q, want %q", got, content)
	}

	// Authorization still holds after the restart: the password check is
	// not something the backend swap can weaken.
	if _, err := client.GetUser("alice", "wrong-password"); err == nil {
		t.Fatal("GetUser after restart accepted the wrong password")
	}
}

// TestSAFEROnMongoTamperedObjectIsRejected confirms the security boundary
// is still SAFER's, not MongoDB's: bytes corrupted in the database must
// fail SAFER's authentication rather than being served as content.
func TestSAFEROnMongoTamperedObjectIsRejected(t *testing.T) {
	cfg, configured, err := mongostore.ConfigFromEnv()
	if err != nil {
		t.Fatalf("bad MongoDB configuration: %v", err)
	}
	if !configured {
		t.Skipf("skipping: %s is not set", mongostore.EnvURI)
	}
	cfg.Database = fmt.Sprintf("safer_e2e_tamper_%d", time.Now().UnixNano())
	ctx := context.Background()

	store, err := mongostore.Open(ctx, cfg)
	if err != nil {
		t.Skipf("skipping: MongoDB is unreachable: %v", err)
	}
	restore := client.UseStorage(store.Storage())
	t.Cleanup(func() {
		restore()
		_ = store.DropDatabase(context.Background())
		_ = store.Close(context.Background())
	})

	alice, err := client.InitUser("alice", "alice-password")
	if err != nil {
		t.Fatalf("InitUser: %v", err)
	}
	if err := alice.StoreFile("target", []byte("authentic content")); err != nil {
		t.Fatalf("StoreFile: %v", err)
	}

	// Corrupt every stored object, the way a compromised database or a
	// malicious operator could.
	ids, err := store.ObjectIDs(ctx)
	if err != nil {
		t.Fatalf("listing object ids: %v", err)
	}
	if len(ids) == 0 {
		t.Fatal("no objects stored, expected SAFER to have written some")
	}
	for _, id := range ids {
		value, found, err := store.Objects().Get(ctx, id)
		if err != nil || !found {
			t.Fatalf("reading %s: found=%v err=%v", id, found, err)
		}
		// Flip a bit rather than replacing wholesale, so the document
		// still looks structurally plausible.
		if len(value) > 0 {
			value[len(value)/2] ^= 0xff
		}
		if err := store.Objects().Put(ctx, id, value); err != nil {
			t.Fatalf("writing tampered %s: %v", id, err)
		}
	}

	if _, err := alice.LoadFile("target"); err == nil {
		t.Fatal("LoadFile returned tampered content instead of an error")
	}
	if _, err := client.GetUser("alice", "alice-password"); err == nil {
		t.Fatal("GetUser accepted a tampered account record")
	}
}
