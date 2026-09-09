// Package mongostore implements SAFER's storage abstraction on MongoDB.
//
// MongoDB's role in SAFER Distributed is narrow and deliberate:
//
//   - shared durable persistence
//   - concurrent access from multiple SAFER workers
//   - persistence across worker restarts
//   - later, atomic multi-document write transactions
//
// MongoDB is NOT the distributed concurrency-control mechanism. It does no
// locking on SAFER's behalf and knows nothing about SAFER's logical
// resources. Strict 2PL, S/X locks, FIFO fairness, transaction leases, and
// fencing tokens stay in SAFER's lock layer (Phase 3). Likewise,
// encryption, MAC/authenticated envelopes, capability and authorization
// semantics, and the Namespace/File resource model stay in the SAFER layer
// above this package.
//
// Consequently this package stores exactly what SAFER already produces:
// opaque encrypted-and-authenticated blobs, addressed by the same logical
// UUIDs SAFER already derives. The cryptographic object layout is not
// redesigned to look more "database-like"; MongoDB sees ciphertext.
package mongostore

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Environment variables read by ConfigFromEnv. Credentials belong in the
// URI supplied by the environment; nothing is hard-coded here.
const (
	EnvURI      = "SAFER_MONGO_URI"
	EnvDatabase = "SAFER_MONGO_DB"
	EnvTimeout  = "SAFER_MONGO_TIMEOUT"
	// EnvTransactionTimeout bounds a whole storage transaction when the
	// caller has no deadline of its own.
	EnvTransactionTimeout = "SAFER_MONGO_TXN_TIMEOUT"
)

// Defaults applied when the corresponding variable is unset.
const (
	DefaultDatabase = "safer"
	DefaultTimeout  = 10 * time.Second
	// DefaultTransactionTimeout bounds one whole storage transaction. It
	// is generous compared with a single operation, because a legitimate
	// multi-object mutation is still one round of writes, and stingy
	// compared with forever, because an unbounded transaction can pin a
	// lock's lease alive indefinitely.
	DefaultTransactionTimeout = 30 * time.Second
)

// Collection names. The schema is intentionally minimal: two collections,
// each keyed by the identifier SAFER already uses.
const (
	ObjectsCollection = "objects"
	KeysCollection    = "public_keys"
)

// Config describes how to reach MongoDB.
type Config struct {
	// URI is the MongoDB connection string, including any credentials.
	URI string
	// Database is the database name. Defaults to DefaultDatabase.
	Database string
	// Timeout bounds connection and per-operation work when the caller's
	// context carries no deadline of its own. Defaults to DefaultTimeout.
	//
	// It is deliberately NOT applied to statements inside a transaction:
	// expiring one statement aborts the whole transaction rather than
	// just that statement.
	Timeout time.Duration

	// TransactionTimeout bounds one whole RunAtomic call when the caller
	// supplied no deadline, so a wedged operation cannot hold a
	// transaction open forever. Defaults to DefaultTransactionTimeout.
	TransactionTimeout time.Duration
}

// ConfigFromEnv builds a Config from the environment.
//
// It reports configured=false, with no error, when SAFER_MONGO_URI is
// unset: that is the normal state for a machine with no MongoDB, and it is
// what lets integration tests skip cleanly while unit tests keep running.
// A malformed value is an error, not a silent fallback.
func ConfigFromEnv() (cfg Config, configured bool, err error) {
	uri := os.Getenv(EnvURI)
	if uri == "" {
		return Config{}, false, nil
	}
	cfg = Config{URI: uri, Database: os.Getenv(EnvDatabase), Timeout: 0}

	if raw := os.Getenv(EnvTimeout); raw != "" {
		d, parseErr := time.ParseDuration(raw)
		if parseErr != nil {
			// Accept a bare number of seconds too; it is an easy thing to
			// write in a shell or a Kubernetes manifest.
			seconds, numErr := strconv.Atoi(raw)
			if numErr != nil {
				return Config{}, false, fmt.Errorf("%s=%q: not a duration: %w", EnvTimeout, raw, parseErr)
			}
			d = time.Duration(seconds) * time.Second
		}
		if d <= 0 {
			return Config{}, false, fmt.Errorf("%s=%q: must be positive", EnvTimeout, raw)
		}
		cfg.Timeout = d
	}

	if raw := os.Getenv(EnvTransactionTimeout); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return Config{}, false, fmt.Errorf("%s=%q: not a duration: %w", EnvTransactionTimeout, raw, err)
		}
		if d <= 0 {
			return Config{}, false, fmt.Errorf("%s=%q: must be positive", EnvTransactionTimeout, raw)
		}
		cfg.TransactionTimeout = d
	}
	return cfg.withDefaults(), true, nil
}

// withDefaults fills in unset optional fields.
func (c Config) withDefaults() Config {
	if c.Database == "" {
		c.Database = DefaultDatabase
	}
	if c.Timeout <= 0 {
		c.Timeout = DefaultTimeout
	}
	if c.TransactionTimeout <= 0 {
		c.TransactionTimeout = DefaultTransactionTimeout
	}
	return c
}

// validate checks the fields that have no sensible default.
func (c Config) validate() error {
	if c.URI == "" {
		return fmt.Errorf("mongostore: %s is required", EnvURI)
	}
	return nil
}
