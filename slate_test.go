package slatedb

import (
	"os"
	"path/filepath"
	"testing"
)

func configureObjectStore(t *testing.T) {
	// If CLOUD_PROVIDER is already set to "aws" we assume the user has provided
	// all other variables and do nothing.
	if os.Getenv("CLOUD_PROVIDER") == "aws" {
		return
	}

	// Look for the standard AWS variables; if they're set we switch to S3.
	if os.Getenv("AWS_ACCESS_KEY_ID") != "" &&
		os.Getenv("AWS_SECRET_ACCESS_KEY") != "" &&
		os.Getenv("AWS_BUCKET") != "" &&
		os.Getenv("AWS_REGION") != "" {

		// make sure CLOUD_PROVIDER is "aws"
		t.Logf("detected AWS_* env vars – running test against S3 bucket %s", os.Getenv("AWS_BUCKET"))
		_ = os.Setenv("CLOUD_PROVIDER", "aws")
		return
	}

	// otherwise use the local provider so the test still runs for developers
	dir := t.TempDir()
	_ = os.Setenv("CLOUD_PROVIDER", "local")
	_ = os.Setenv("LOCAL_PATH", filepath.Join(dir, "store"))
	t.Logf("falling back to local object-store at %s", os.Getenv("LOCAL_PATH"))
}

func TestPutIterate(t *testing.T) {
	configureObjectStore(t)

	dbPath := "demo_db"

	h := Open(dbPath)
	defer Close(h)

	data := map[string]string{
		"user:1": "Alice",
		"user:2": "Bob",
	}

	for k, v := range data {
		Put(h, []byte(k), []byte(v))
	}

	kvs := IterateAll(h)
	if len(kvs) != len(data) {
		t.Fatalf("expected %d records, got %d", len(data), len(kvs))
	}

	got := make(map[string]string)
	for _, kv := range kvs {
		got[string(kv.Key)] = string(kv.Val)
	}
	for k, v := range data {
		if got[k] != v {
			t.Fatalf("key %q: want %q, got %q", k, v, got[k])
		}
	}
}
