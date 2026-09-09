// Command saferworker is a single-operation SAFER worker process, used by
// the cross-process integration harness.
//
// It is a real, independent worker: its own OS process, its own MongoDB
// connection, its own gRPC connection to the lock coordinator, and its own
// SAFER client state. Nothing is shared with the test process except the
// database and the coordinator -- which is the whole point, since
// goroutines in one process would prove nothing about distribution.
//
// It performs exactly one SAFER operation and reports the result as a
// single JSON line on stdout, so the harness can assert on outcomes rather
// than parse logs.
//
// Overlap between workers is arranged with an explicit barrier, never a
// sleep: each worker connects to the harness's barrier address and blocks
// until every participant has arrived, so the operations genuinely race.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/JamJamzzz/safer-distributed/client"
	"github.com/JamJamzzz/safer-distributed/client/coordination/grpccoord"
	"github.com/JamJamzzz/safer-distributed/client/storage/mongostore"
)

// result is the JSON line this worker prints. Every field is filled in on
// every path, so the harness never has to distinguish "absent" from
// "zero".
type result struct {
	Worker     string `json:"worker"`
	Op         string `json:"op"`
	OK         bool   `json:"ok"`
	Error      string `json:"error,omitempty"`
	Content    string `json:"content"`
	Version    uint64 `json:"version"`
	ChunkCount uint64 `json:"chunk_count"`
	StartedAt  int64  `json:"started_at_unix_nano"`
	EndedAt    int64  `json:"ended_at_unix_nano"`
}

func main() {
	var (
		name     = flag.String("name", "worker", "worker name, echoed in the result")
		op       = flag.String("op", "", "operation: append, store, load, or metadata")
		username = flag.String("user", "", "SAFER username")
		password = flag.String("password", "", "SAFER password")
		filename = flag.String("file", "", "SAFER filename")
		content  = flag.String("content", "", "content to write, for append and store")
		barrier  = flag.String("barrier", "", "barrier address to rendezvous at before the operation")
		database = flag.String("db", "", "MongoDB database name; overrides SAFER_MONGO_DB")
	)
	flag.Parse()

	res := result{Worker: *name, Op: *op}
	if err := run(&res, *op, *username, *password, *filename, *content, *barrier, *database); err != nil {
		res.OK = false
		res.Error = err.Error()
	} else {
		res.OK = true
	}

	encoded, err := json.Marshal(res)
	if err != nil {
		fmt.Fprintf(os.Stderr, "saferworker: encoding result: %v\n", err)
		os.Exit(2)
	}
	fmt.Println(string(encoded))

	// A failed SAFER operation is a reportable outcome, not a crash: the
	// harness asserts on the JSON. Exit non-zero only when the result
	// could not be produced at all.
	os.Exit(0)
}

func run(res *result, op, username, password, filename, content, barrier, database string) error {
	ctx := context.Background()

	// Storage: this worker's own MongoDB connection.
	mongoCfg, configured, err := mongostore.ConfigFromEnv()
	if err != nil {
		return fmt.Errorf("mongo configuration: %w", err)
	}
	if !configured {
		return fmt.Errorf("%s is not set", mongostore.EnvURI)
	}
	if database != "" {
		mongoCfg.Database = database
	}
	store, err := mongostore.Open(ctx, mongoCfg)
	if err != nil {
		return fmt.Errorf("opening mongo: %w", err)
	}
	defer func() { _ = store.Close(context.Background()) }()
	client.UseStorage(store.Storage())

	// Coordination: this worker's own connection to the lock
	// coordinator. Without it the worker would coordinate only with
	// itself, which is exactly the V1 limitation being fixed.
	coordCfg, configured, err := grpccoord.ConfigFromEnv()
	if err != nil {
		return fmt.Errorf("coordinator configuration: %w", err)
	}
	if !configured {
		return fmt.Errorf("%s is not set", grpccoord.EnvAddress)
	}
	backend, err := grpccoord.Dial(ctx, coordCfg)
	if err != nil {
		return fmt.Errorf("dialing coordinator: %w", err)
	}
	defer func() { _ = backend.Close() }()
	client.UseCoordination(backend)

	user, err := client.GetUser(username, password)
	if err != nil {
		return fmt.Errorf("GetUser: %w", err)
	}

	// Everything above is setup, and setup times vary between processes.
	// The barrier is crossed here, immediately before the operation, so
	// the operations themselves overlap rather than the process launches.
	if barrier != "" {
		if err := waitAtBarrier(barrier, res.Worker); err != nil {
			return fmt.Errorf("barrier: %w", err)
		}
	}

	res.StartedAt = time.Now().UnixNano()
	defer func() { res.EndedAt = time.Now().UnixNano() }()

	switch op {
	case "append":
		if err := user.AppendToFile(filename, []byte(content)); err != nil {
			return fmt.Errorf("AppendToFile: %w", err)
		}
	case "store":
		if err := user.StoreFile(filename, []byte(content)); err != nil {
			return fmt.Errorf("StoreFile: %w", err)
		}
	case "load":
		loaded, err := user.LoadFile(filename)
		if err != nil {
			return fmt.Errorf("LoadFile: %w", err)
		}
		res.Content = string(loaded)
	case "metadata":
		// no mutation; the snapshot below is the whole point
	default:
		return fmt.Errorf("unknown op %q", op)
	}

	snapshot, err := user.ReadFileMetadata(filename)
	if err != nil {
		return fmt.Errorf("ReadFileMetadata: %w", err)
	}
	res.Version = snapshot.Version
	res.ChunkCount = snapshot.ChunkCount
	return nil
}

// waitAtBarrier connects to the harness's barrier, announces this worker,
// and blocks until the harness releases every participant at once.
//
// This is an explicit rendezvous, not a timing guess: the harness knows
// how many workers it started and releases them only when all have
// arrived.
func waitAtBarrier(address, name string) error {
	conn, err := net.DialTimeout("tcp", address, 30*time.Second)
	if err != nil {
		return fmt.Errorf("dialing %s: %w", address, err)
	}
	defer func() { _ = conn.Close() }()

	if err := conn.SetDeadline(time.Now().Add(60 * time.Second)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(conn, "%s\n", name); err != nil {
		return fmt.Errorf("announcing: %w", err)
	}
	// Blocks until the harness writes the release line.
	release, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return fmt.Errorf("waiting for release: %w", err)
	}
	if release != "go\n" {
		return fmt.Errorf("unexpected release message %q", release)
	}
	return nil
}
