//go:build toxic

// Requires env vars: SLATEDB_S3_URL, TOXIPROXY_ADMIN_URL, plus AWS creds.
// See scripts/run_go_timeout_tests.sh.

package slatedb_test

import (
	"errors"
	"fmt"
	"os"
	"testing"

	slatedb "slatedb.io/slatedb-go/uniffi"
)

func openS3Store(t *testing.T) *slatedb.ObjectStore {
	t.Helper()

	s3URL := os.Getenv("SLATEDB_S3_URL")
	if s3URL == "" {
		t.Skip("SLATEDB_S3_URL not set; skipping S3-backed test")
	}

	store, err := slatedb.ObjectStoreResolve(s3URL)
	if err != nil {
		t.Fatalf("ObjectStoreResolve(%q): %v", s3URL, err)
	}
	t.Cleanup(store.Destroy)
	return store
}

func seedS3Data(handle *testDB) error {
	for i := range 100 {
		key := fmt.Sprintf("timeout-key:%04d", i)
		val := fmt.Sprintf("timeout-val:%04d", i)
		if _, err := handle.db.Put([]byte(key), []byte(val)); err != nil {
			return err
		}
	}
	if err := handle.db.Flush(); err != nil {
		return err
	}
	return nil
}

func TestPutWithTimeoutTripped(t *testing.T) {
	store := openS3Store(t)
	handle := openTestDB(t, store, nil)
	seedS3Data(handle)

	_, err := handle.db.PutWithOptions(
		[]byte("foo"),
		[]byte("bar"),
		slatedb.PutOptions{
			Ttl: slatedb.TtlDefault{},
		},
		slatedb.WriteOptions{
			TimeoutMs:    uint64Ptr(1),
			AwaitDurable: true,
		},
	)

	if err == nil || !errors.Is(err, slatedb.ErrErrorTimeout) {
		t.Fatalf("PutWithOptions(timeout=100ms): got %v, want ErrErrorTimeout", err)
	}
}
