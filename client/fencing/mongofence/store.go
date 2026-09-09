// Package mongofence stores SAFER's fencing metadata in MongoDB.
//
// This is the coordinator's side of fencing: allocating a token when an
// exclusive lock is granted, and invalidating it when the grant ends. The
// worker's side -- proving at commit time that its token is still current
// -- lives in the storage backend, because it has to happen inside the
// worker's own storage transaction.
//
// The collection holds coordination metadata only. It contains no SAFER
// object content, no ciphertext and no keys: just resource identifiers,
// counters, and owner ids.
package mongofence

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

	"github.com/JamJamzzz/safer-distributed/client/fencing"
)

// Store is the MongoDB-backed fence store.
type Store struct {
	client     *mongo.Client
	collection *mongo.Collection
	timeout    time.Duration
}

// Config describes how to reach the fence collection. It is the same
// deployment SAFER's objects live in; the collection is separate.
type Config struct {
	URI      string
	Database string
	Timeout  time.Duration
}

const defaultTimeout = 10 * time.Second

// Open connects and verifies the connection.
func Open(ctx context.Context, cfg Config) (*Store, error) {
	if cfg.URI == "" {
		return nil, errors.New("mongofence: a MongoDB URI is required")
	}
	if cfg.Database == "" {
		return nil, errors.New("mongofence: a database name is required")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultTimeout
	}

	connectCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	client, err := mongo.Connect(connectCtx, options.Client().ApplyURI(cfg.URI))
	if err != nil {
		return nil, fmt.Errorf("mongofence: connect: %w", err)
	}
	if err := client.Ping(connectCtx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, fmt.Errorf("mongofence: ping: %w", err)
	}

	// Majority write concern: a token the coordinator has handed out must
	// not be able to disappear. Fencing is only meaningful if the
	// allocation that issued a token is at least as durable as the writes
	// that token guards.
	collection := client.Database(cfg.Database).Collection(
		fencing.FencesCollection,
		options.Collection().
			SetWriteConcern(writeconcern.New(writeconcern.WMajority())).
			SetReadConcern(readconcern.Majority()),
	)
	return &Store{client: client, collection: collection, timeout: cfg.Timeout}, nil
}

// Close releases the connection pool.
func (s *Store) Close(ctx context.Context) error {
	closeCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.client.Disconnect(closeCtx)
}

// Ping reports whether the fence store is reachable. The coordinator uses
// it to tell "cleanup failed because storage is down" from "cleanup failed
// for some other reason".
func (s *Store) Ping(ctx context.Context) error {
	pingCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.client.Ping(pingCtx, nil)
}

// Allocate durably advances a resource's fencing token and records the
// transaction that now owns it.
//
// The increment and the ownership change are one atomic document update,
// so two concurrent allocations cannot produce the same token. The new
// token is returned, and the caller must not hand out an exclusive grant
// until this has succeeded: an unfenced exclusive grant is a writer that
// nothing can later stop.
func (s *Store) Allocate(ctx context.Context, resource string, ownerTxn string) (fencing.Token, error) {
	opCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	var updated struct {
		Token int64 `bson:"token"`
	}
	err := s.collection.FindOneAndUpdate(
		opCtx,
		bson.M{"_id": resource},
		bson.M{
			"$inc": bson.M{fencing.FieldToken: int64(1)},
			"$set": bson.M{
				fencing.FieldOwnerTxn:  ownerTxn,
				fencing.FieldUpdatedAt: time.Now().UTC(),
			},
		},
		options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After),
	).Decode(&updated)
	if err != nil {
		return 0, fmt.Errorf("mongofence: allocating a token for %s: %w", resource, err)
	}
	if updated.Token <= 0 {
		return 0, fmt.Errorf("mongofence: allocated a non-positive token %d for %s", updated.Token, resource)
	}
	return fencing.Token(updated.Token), nil
}

// Invalidate clears a transaction's ownership of a resource's fence.
//
// The token is deliberately NOT reset: tokens only ever move forward, so
// that a token issued in the past can never become current again. Clearing
// ownership is what makes a still-running holder's conditional commit fail
// -- its owner_txn no longer matches, even if no one else has taken the
// lock yet.
//
// It is idempotent. Invalidating a fence this transaction does not own
// leaves the document untouched and reports success, so a retried cleanup
// cannot clobber a newer owner.
func (s *Store) Invalidate(ctx context.Context, resource string, ownerTxn string) error {
	opCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	_, err := s.collection.UpdateOne(
		opCtx,
		bson.M{"_id": resource, fencing.FieldOwnerTxn: ownerTxn},
		bson.M{"$set": bson.M{
			fencing.FieldOwnerTxn:  nil,
			fencing.FieldUpdatedAt: time.Now().UTC(),
		}},
	)
	if err != nil {
		return fmt.Errorf("mongofence: invalidating the fence for %s: %w", resource, err)
	}
	return nil
}

// InvalidateAll clears every listed grant, stopping at the first failure.
//
// Stopping matters: the caller must not release logical locks while any
// fence is still owned by the transaction being cleaned up, so a partial
// failure has to be reported as a failure.
func (s *Store) InvalidateAll(ctx context.Context, grants []fencing.Grant) error {
	for _, grant := range grants {
		if err := s.Invalidate(ctx, grant.Resource, grant.OwnerTxn); err != nil {
			return err
		}
	}
	return nil
}

// Current reads a resource's fence document. Diagnostic and test use only:
// nothing in the protocol makes a decision from a plain read, because a
// read cannot create the write conflict that fencing depends on.
func (s *Store) Current(ctx context.Context, resource string) (token fencing.Token, ownerTxn string, found bool, err error) {
	opCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	var doc struct {
		Token    int64   `bson:"token"`
		OwnerTxn *string `bson:"owner_txn"`
	}
	err = s.collection.FindOne(opCtx, bson.M{"_id": resource}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return 0, "", false, nil
	}
	if err != nil {
		return 0, "", false, fmt.Errorf("mongofence: reading the fence for %s: %w", resource, err)
	}
	owner := ""
	if doc.OwnerTxn != nil {
		owner = *doc.OwnerTxn
	}
	return fencing.Token(doc.Token), owner, true, nil
}
