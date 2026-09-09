package fencing

import (
	"context"
	"errors"
	"testing"
)

func TestResourceKeyDistinguishesTypes(t *testing.T) {
	// A namespace resource and a file resource that happen to share a key
	// must not collide onto one fence document; if they did, one would
	// silently fence the other.
	namespace := ResourceKey(0, "same-key")
	file := ResourceKey(1, "same-key")
	if namespace == file {
		t.Fatalf("resource types collide: both produced %q", namespace)
	}
	if got := ResourceKey(1, "same-key"); got != file {
		t.Fatalf("ResourceKey is not deterministic: %q then %q", file, got)
	}
}

func TestGrantsRoundTripThroughContext(t *testing.T) {
	ctx := context.Background()
	if got := GrantsFromContext(ctx); len(got) != 0 {
		t.Fatalf("empty context carried %d grants", len(got))
	}

	grants := []Grant{
		{Resource: ResourceKey(0, "ns"), Token: 7, OwnerTxn: "txn-a"},
		{Resource: ResourceKey(1, "file"), Token: 9, OwnerTxn: "txn-a"},
	}
	ctx = WithGrants(ctx, grants)

	got := GrantsFromContext(ctx)
	if len(got) != 2 || got[0].Token != 7 || got[1].Token != 9 {
		t.Fatalf("grants did not round-trip: %+v", got)
	}
}

// TestGrantsAreCopiedIntoContext checks that a later change to the
// caller's slice cannot change what an in-flight transaction validates.
func TestGrantsAreCopiedIntoContext(t *testing.T) {
	grants := []Grant{{Resource: "r", Token: 1, OwnerTxn: "txn"}}
	ctx := WithGrants(context.Background(), grants)

	grants[0].Token = 999

	got := GrantsFromContext(ctx)
	if got[0].Token != 1 {
		t.Fatalf("mutating the caller's slice changed the context's grants: token %d", got[0].Token)
	}
}

// TestGrantsAreScopedPerContext is the property that replaces global
// state: two concurrent operations each see only their own grants.
func TestGrantsAreScopedPerContext(t *testing.T) {
	base := context.Background()
	first := WithGrants(base, []Grant{{Resource: "r", Token: 1, OwnerTxn: "txn-a"}})
	second := WithGrants(base, []Grant{{Resource: "r", Token: 2, OwnerTxn: "txn-b"}})

	if got := GrantsFromContext(first); got[0].OwnerTxn != "txn-a" || got[0].Token != 1 {
		t.Fatalf("first context sees %+v", got)
	}
	if got := GrantsFromContext(second); got[0].OwnerTxn != "txn-b" || got[0].Token != 2 {
		t.Fatalf("second context sees %+v", got)
	}
	if got := GrantsFromContext(base); len(got) != 0 {
		t.Fatalf("the parent context was modified: %+v", got)
	}
}

func TestValidationSkipIsOptInAndScoped(t *testing.T) {
	base := context.Background()
	if ValidationSkippedForTest(base) {
		t.Fatal("validation is skipped by default")
	}
	skipped := WithoutValidationForTest(base)
	if !ValidationSkippedForTest(skipped) {
		t.Fatal("WithoutValidationForTest had no effect")
	}
	// It must not leak to a sibling context: the negative control has to
	// be unable to weaken any other operation.
	if ValidationSkippedForTest(base) {
		t.Fatal("skipping validation leaked to the parent context")
	}
}

func TestStaleFenceErrorIsDistinguishable(t *testing.T) {
	// Callers need to tell "your work was correctly rejected because you
	// lost the lock" from "storage failed".
	wrapped := errors.Join(errors.New("mongostore: transaction"), ErrStaleFence)
	if !errors.Is(wrapped, ErrStaleFence) {
		t.Fatal("a wrapped stale-fence error is not detectable with errors.Is")
	}
	if errors.Is(errors.New("some other failure"), ErrStaleFence) {
		t.Fatal("an unrelated error matched ErrStaleFence")
	}
}
