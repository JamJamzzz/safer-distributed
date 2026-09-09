package mongostore

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"errors"
	"fmt"
	"time"

	userlib "github.com/cs161-staff/project2-userlib"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	"github.com/cs161-staff/project2-starter-code/client/storage"
)

// Store is a MongoDB-backed SAFER storage backend. It is safe for
// concurrent use: the MongoDB driver pools and synchronizes connections
// itself.
//
// Note what is deliberately absent: the datastoreMu/keystoreMu latches of
// the userlib backend. Those exist only because userlib's Go maps are
// unsynchronized in-process state. Imposing a process-wide mutex here
// would serialize a backend whose entire purpose is concurrent shared
// access, and would not coordinate anything across processes anyway.
type Store struct {
	client  *mongo.Client
	timeout time.Duration
	objects *ObjectStore
	keys    *KeyStore
}

// ObjectStore persists SAFER's encrypted objects in the objects
// collection.
type ObjectStore struct {
	collection *mongo.Collection
	timeout    time.Duration
}

// KeyStore persists public-key material in the public_keys collection.
type KeyStore struct {
	collection *mongo.Collection
	timeout    time.Duration
}

var (
	_ storage.ObjectStore = (*ObjectStore)(nil)
	_ storage.KeyStore    = (*KeyStore)(nil)
)

// objectDocument is the objects schema:
//
//	{ _id: "<uuid>", value: <binary> }
//
// _id is the canonical string form of the same logical uuid.UUID SAFER
// already derives, so the addressing scheme is unchanged; it is stored as
// a string rather than a BSON UUID binary purely so documents stay
// readable during operations. value is the opaque SAFER envelope:
// ciphertext plus its MAC, produced and verified entirely above this
// layer.
type objectDocument struct {
	ID    string           `bson:"_id"`
	Value primitive.Binary `bson:"value"`
}

// keyDocument is the public_keys schema:
//
//	{ _id: "<key name>", key_type: "PKE"|"DS", public_key: <DER binary> }
//
// public_key is the PKIX DER encoding of the RSA public key -- the
// standard interchange form, rather than a driver-specific struct dump, so
// the stored material stays meaningful to anything else that reads this
// collection.
type keyDocument struct {
	ID        string           `bson:"_id"`
	KeyType   string           `bson:"key_type"`
	PublicKey primitive.Binary `bson:"public_key"`
}

// Open connects to MongoDB and verifies the connection with a ping, so
// that a misconfigured deployment fails at startup rather than at the
// first SAFER operation.
func Open(ctx context.Context, cfg Config) (*Store, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	connectCtx, cancel := withTimeout(ctx, cfg.Timeout)
	defer cancel()

	client, err := mongo.Connect(connectCtx, options.Client().ApplyURI(cfg.URI))
	if err != nil {
		return nil, fmt.Errorf("mongostore: connect: %w", err)
	}
	if err := client.Ping(connectCtx, readpref.Primary()); err != nil {
		// Do not leak the pool if the server is unreachable.
		_ = client.Disconnect(context.Background())
		return nil, fmt.Errorf("mongostore: ping: %w", err)
	}

	db := client.Database(cfg.Database)
	return &Store{
		client:  client,
		timeout: cfg.Timeout,
		objects: &ObjectStore{collection: db.Collection(ObjectsCollection), timeout: cfg.Timeout},
		keys:    &KeyStore{collection: db.Collection(KeysCollection), timeout: cfg.Timeout},
	}, nil
}

// Storage returns the bundle SAFER's client layer installs.
func (s *Store) Storage() storage.Storage {
	return storage.Storage{Objects: s.objects, Keys: s.keys}
}

// Objects returns the object half of the backend.
func (s *Store) Objects() *ObjectStore { return s.objects }

// Keys returns the key half of the backend.
func (s *Store) Keys() *KeyStore { return s.keys }

// Ping reports whether the backend is currently reachable.
func (s *Store) Ping(ctx context.Context) error {
	pingCtx, cancel := withTimeout(ctx, s.timeout)
	defer cancel()
	if err := s.client.Ping(pingCtx, readpref.Primary()); err != nil {
		return fmt.Errorf("mongostore: ping: %w", err)
	}
	return nil
}

// ObjectIDs lists the logical UUIDs of every stored object.
//
// This is an administrative and diagnostic helper -- backup tooling,
// operational inspection, and tamper tests. SAFER's own operations never
// scan the collection: they address objects by UUIDs they derive
// themselves.
func (s *Store) ObjectIDs(ctx context.Context) ([]uuid.UUID, error) {
	opCtx, cancel := withTimeout(ctx, s.timeout)
	defer cancel()

	cursor, err := s.objects.collection.Find(opCtx, bson.M{}, options.Find().SetProjection(bson.M{"_id": 1}))
	if err != nil {
		return nil, fmt.Errorf("mongostore: list object ids: %w", err)
	}
	defer cursor.Close(opCtx)

	var ids []uuid.UUID
	for cursor.Next(opCtx) {
		var doc struct {
			ID string `bson:"_id"`
		}
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("mongostore: list object ids: %w", err)
		}
		id, err := uuid.Parse(doc.ID)
		if err != nil {
			return nil, fmt.Errorf("mongostore: list object ids: bad _id %q: %w", doc.ID, err)
		}
		ids = append(ids, id)
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("mongostore: list object ids: %w", err)
	}
	return ids, nil
}

// DropDatabase permanently deletes every SAFER object and key in this
// store's database.
//
// It exists for test isolation and for operational teardown of a
// throwaway database. It is destructive and irreversible; nothing in
// SAFER's normal operation calls it.
func (s *Store) DropDatabase(ctx context.Context) error {
	opCtx, cancel := withTimeout(ctx, s.timeout)
	defer cancel()

	if err := s.objects.collection.Database().Drop(opCtx); err != nil {
		return fmt.Errorf("mongostore: drop database: %w", err)
	}
	return nil
}

// Close releases the connection pool.
func (s *Store) Close(ctx context.Context) error {
	closeCtx, cancel := withTimeout(ctx, s.timeout)
	defer cancel()
	if err := s.client.Disconnect(closeCtx); err != nil {
		return fmt.Errorf("mongostore: disconnect: %w", err)
	}
	return nil
}

// withTimeout bounds an operation without overriding a deadline the caller
// already chose.
func withTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok || timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, timeout)
}

// Get returns the stored envelope for id.
//
// An absent id reports found=false with a nil error; only a backend
// failure produces an error. That distinction matters: SAFER treats
// absence as a meaningful, expected answer, so a failure must never be
// disguised as one.
func (o *ObjectStore) Get(ctx context.Context, id uuid.UUID) ([]byte, bool, error) {
	opCtx, cancel := withTimeout(ctx, o.timeout)
	defer cancel()

	var doc objectDocument
	err := o.collection.FindOne(opCtx, bson.M{"_id": id.String()}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("mongostore: get object %s: %w", id, err)
	}
	return doc.Value.Data, true, nil
}

// Put stores value under id, overwriting any previous value, matching the
// overwrite semantics SAFER's in-place object updates rely on.
func (o *ObjectStore) Put(ctx context.Context, id uuid.UUID, value []byte) error {
	opCtx, cancel := withTimeout(ctx, o.timeout)
	defer cancel()

	// A nil payload must round-trip as an empty, present object rather
	// than as BSON null.
	if value == nil {
		value = []byte{}
	}
	doc := objectDocument{
		ID:    id.String(),
		Value: primitive.Binary{Subtype: bson.TypeBinaryGeneric, Data: value},
	}
	_, err := o.collection.ReplaceOne(opCtx, bson.M{"_id": doc.ID}, doc, options.Replace().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("mongostore: put object %s: %w", id, err)
	}
	return nil
}

// Delete removes id. Deleting an absent id is not an error, matching the
// userlib backend.
func (o *ObjectStore) Delete(ctx context.Context, id uuid.UUID) error {
	opCtx, cancel := withTimeout(ctx, o.timeout)
	defer cancel()

	if _, err := o.collection.DeleteOne(opCtx, bson.M{"_id": id.String()}); err != nil {
		return fmt.Errorf("mongostore: delete object %s: %w", id, err)
	}
	return nil
}

// Get returns the public key registered under name.
func (k *KeyStore) Get(ctx context.Context, name string) (userlib.PublicKeyType, bool, error) {
	opCtx, cancel := withTimeout(ctx, k.timeout)
	defer cancel()

	var doc keyDocument
	err := k.collection.FindOne(opCtx, bson.M{"_id": name}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return userlib.PublicKeyType{}, false, nil
	}
	if err != nil {
		return userlib.PublicKeyType{}, false, fmt.Errorf("mongostore: get key %q: %w", name, err)
	}

	key, err := decodePublicKey(doc)
	if err != nil {
		return userlib.PublicKeyType{}, false, fmt.Errorf("mongostore: get key %q: %w", name, err)
	}
	return key, true, nil
}

// Put registers key under name.
//
// Registration is write-once, as it is in userlib: SAFER's identity
// semantics depend on a claimed key name staying claimed. The guarantee is
// enforced by MongoDB's unique _id index via InsertOne, so it holds across
// every worker in the deployment, not merely within one process.
func (k *KeyStore) Put(ctx context.Context, name string, key userlib.PublicKeyType) error {
	opCtx, cancel := withTimeout(ctx, k.timeout)
	defer cancel()

	doc, err := encodePublicKey(name, key)
	if err != nil {
		return fmt.Errorf("mongostore: put key %q: %w", name, err)
	}
	if _, err := k.collection.InsertOne(opCtx, doc); err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return fmt.Errorf("mongostore: put key %q: that key name is already taken", name)
		}
		return fmt.Errorf("mongostore: put key %q: %w", name, err)
	}
	return nil
}

func encodePublicKey(name string, key userlib.PublicKeyType) (keyDocument, error) {
	der, err := x509.MarshalPKIXPublicKey(&key.PubKey)
	if err != nil {
		return keyDocument{}, fmt.Errorf("encode public key: %w", err)
	}
	return keyDocument{
		ID:        name,
		KeyType:   key.KeyType,
		PublicKey: primitive.Binary{Subtype: bson.TypeBinaryGeneric, Data: der},
	}, nil
}

func decodePublicKey(doc keyDocument) (userlib.PublicKeyType, error) {
	parsed, err := x509.ParsePKIXPublicKey(doc.PublicKey.Data)
	if err != nil {
		return userlib.PublicKeyType{}, fmt.Errorf("decode public key: %w", err)
	}
	rsaKey, ok := parsed.(*rsa.PublicKey)
	if !ok {
		return userlib.PublicKeyType{}, fmt.Errorf("decode public key: got %T, want *rsa.PublicKey", parsed)
	}
	return userlib.PublicKeyType{KeyType: doc.KeyType, PubKey: *rsaKey}, nil
}
