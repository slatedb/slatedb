package slatedb

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"testing"
)

type counterMergeOperator struct{}

func (counterMergeOperator) Merge(_ []byte, existingValue *[]byte, value []byte) []byte {
	var existing uint64
	if existingValue != nil {
		existing = binary.LittleEndian.Uint64(*existingValue)
	}
	return binary.LittleEndian.AppendUint64(nil, existing+binary.LittleEndian.Uint64(value))
}

func ptrBytes(value []byte) *[]byte {
	copyValue := append([]byte(nil), value...)
	return &copyValue
}

func requireMemoryStore(t *testing.T) *ObjectStore {
	t.Helper()

	store, err := ResolveObjectStore("memory:///")
	if err != nil {
		t.Fatalf("ResolveObjectStore(memory): %v", err)
	}

	return store
}

func requireBuilder(t *testing.T, path string, store *ObjectStore) *DbBuilder {
	t.Helper()

	builder := NewDbBuilder(path, store)
	if builder == nil {
		t.Fatal("NewDbBuilder returned nil")
	}

	return builder
}

func requireDb(t *testing.T, builder *DbBuilder) *Db {
	t.Helper()

	db, err := builder.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	return db
}

func TestResolveMemoryObjectStoreAndBuilderLifecycle(t *testing.T) {
	settingsJSON, err := DefaultSettingsJson()
	if err != nil {
		t.Fatalf("DefaultSettingsJson: %v", err)
	}
	if !json.Valid([]byte(settingsJSON)) {
		t.Fatalf("default settings are not valid json: %q", settingsJSON)
	}

	store := requireMemoryStore(t)
	defer store.Destroy()

	builder := requireBuilder(t, "builder-lifecycle", store)
	defer builder.Destroy()

	if err := builder.WithSettingsJson(settingsJSON); err != nil {
		t.Fatalf("WithSettingsJson(default): %v", err)
	}

	db := requireDb(t, builder)
	defer db.Destroy()
	defer func() { _ = db.Close() }()

	if err := db.Status(); err != nil {
		t.Fatalf("Status before close: %v", err)
	}

	if err := builder.WithSeed(7); !errors.Is(err, ErrSlatedbErrorInvalid) {
		t.Fatalf("WithSeed after Build() = %v, want invalid error", err)
	}
}

func TestPutGetSnapshotAndClose(t *testing.T) {
	store := requireMemoryStore(t)
	defer store.Destroy()

	builder := requireBuilder(t, "snapshot-roundtrip", store)
	defer builder.Destroy()

	db := requireDb(t, builder)
	defer db.Destroy()
	defer func() { _ = db.Close() }()

	if _, err := db.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("Put(k1): %v", err)
	}

	snapshot, err := db.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer snapshot.Destroy()

	if _, err := db.Put([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("Put(k2): %v", err)
	}

	value, err := db.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("Get(k1): %v", err)
	}
	if value == nil || string(*value) != "v1" {
		t.Fatalf("Get(k1) = %#v, want v1", value)
	}

	value, err = db.Get([]byte("k2"))
	if err != nil {
		t.Fatalf("Get(k2): %v", err)
	}
	if value == nil || string(*value) != "v2" {
		t.Fatalf("Get(k2) = %#v, want v2", value)
	}

	snapshotValue, err := snapshot.Get([]byte("k2"))
	if err != nil {
		t.Fatalf("Snapshot.Get(k2): %v", err)
	}
	if snapshotValue != nil {
		t.Fatalf("Snapshot.Get(k2) = %#v, want nil", snapshotValue)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := db.Status(); !errors.Is(err, ErrSlatedbErrorClosed) {
		t.Fatalf("Status after Close() = %v, want closed error", err)
	}
}

func TestMergeOperatorRoundTrip(t *testing.T) {
	store := requireMemoryStore(t)
	defer store.Destroy()

	builder := requireBuilder(t, "merge-roundtrip", store)
	defer builder.Destroy()

	if err := builder.WithMergeOperator(counterMergeOperator{}); err != nil {
		t.Fatalf("WithMergeOperator: %v", err)
	}

	db := requireDb(t, builder)
	defer db.Destroy()
	defer func() { _ = db.Close() }()

	if _, err := db.Put([]byte("counter"), binary.LittleEndian.AppendUint64(nil, 1)); err != nil {
		t.Fatalf("Put(counter): %v", err)
	}
	if _, err := db.Merge([]byte("counter"), binary.LittleEndian.AppendUint64(nil, 2)); err != nil {
		t.Fatalf("Merge(counter): %v", err)
	}

	value, err := db.Get([]byte("counter"))
	if err != nil {
		t.Fatalf("Get(counter): %v", err)
	}
	if value == nil {
		t.Fatal("Get(counter) returned nil")
	}
	if got := binary.LittleEndian.Uint64(*value); got != 3 {
		t.Fatalf("counter = %d, want 3", got)
	}
}

func TestIteratorAndTransactionLifecycle(t *testing.T) {
	store := requireMemoryStore(t)
	defer store.Destroy()

	builder := requireBuilder(t, "iterator-transaction", store)
	defer builder.Destroy()

	db := requireDb(t, builder)
	defer db.Destroy()
	defer func() { _ = db.Close() }()

	for key, value := range map[string]string{
		"a": "1",
		"b": "2",
		"c": "3",
	} {
		if _, err := db.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Put(%s): %v", key, err)
		}
	}

	iter, err := db.Scan(DbKeyRange{
		Start:          ptrBytes([]byte("a")),
		StartInclusive: true,
		End:            ptrBytes([]byte("z")),
		EndInclusive:   false,
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	defer iter.Destroy()

	first, err := iter.Next()
	if err != nil {
		t.Fatalf("Next(first): %v", err)
	}
	if first == nil || string(first.Key) != "a" {
		t.Fatalf("first key = %#v, want a", first)
	}

	if err := iter.Seek([]byte("c")); err != nil {
		t.Fatalf("Seek(c): %v", err)
	}

	third, err := iter.Next()
	if err != nil {
		t.Fatalf("Next(after seek): %v", err)
	}
	if third == nil || string(third.Key) != "c" {
		t.Fatalf("key after seek = %#v, want c", third)
	}

	last, err := iter.Next()
	if err != nil {
		t.Fatalf("Next(exhausted): %v", err)
	}
	if last != nil {
		t.Fatalf("Next(exhausted) = %#v, want nil", last)
	}

	if err := iter.Seek([]byte{}); !errors.Is(err, ErrSlatedbErrorInvalid) {
		t.Fatalf("Seek(empty) = %v, want invalid error", err)
	}

	txn, err := db.Begin(IsolationLevelSnapshot)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer txn.Destroy()

	if err := txn.Put([]byte("tx"), []byte("value")); err != nil {
		t.Fatalf("Txn.Put: %v", err)
	}

	txnValue, err := txn.Get([]byte("tx"))
	if err != nil {
		t.Fatalf("Txn.Get: %v", err)
	}
	if txnValue == nil || string(*txnValue) != "value" {
		t.Fatalf("Txn.Get(tx) = %#v, want value", txnValue)
	}

	handle, err := txn.Commit()
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if handle == nil || handle.Seqnum == 0 {
		t.Fatalf("Commit handle = %#v, want non-nil with sequence number", handle)
	}

	committedValue, err := db.Get([]byte("tx"))
	if err != nil {
		t.Fatalf("Get(committed tx): %v", err)
	}
	if committedValue == nil || string(*committedValue) != "value" {
		t.Fatalf("Get(committed tx) = %#v, want value", committedValue)
	}

	if _, err := txn.Commit(); !errors.Is(err, ErrSlatedbErrorInvalid) {
		t.Fatalf("Commit after completion = %v, want invalid error", err)
	}
}

func TestInvalidSettingsJson(t *testing.T) {
	store := requireMemoryStore(t)
	defer store.Destroy()

	builder := requireBuilder(t, "invalid-settings", store)
	defer builder.Destroy()

	if err := builder.WithSettingsJson("{not-json}"); !errors.Is(err, ErrSlatedbErrorInvalid) {
		t.Fatalf("WithSettingsJson(invalid) = %v, want invalid error", err)
	}
}

func TestFileObjectStoreRoundTrip(t *testing.T) {
	tempDir := t.TempDir()
	storeURL := (&url.URL{
		Scheme: "file",
		Path:   filepath.ToSlash(tempDir),
	}).String()

	store, err := ResolveObjectStore(storeURL)
	if err != nil {
		t.Fatalf("ResolveObjectStore(file): %v", err)
	}
	defer store.Destroy()

	builder := requireBuilder(t, "file-roundtrip", store)
	defer builder.Destroy()

	db := requireDb(t, builder)
	defer db.Destroy()
	defer func() { _ = db.Close() }()

	if _, err := db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put(file): %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush(file): %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close(file): %v", err)
	}

	value, err := db.Get([]byte("k"))
	if err == nil {
		t.Fatalf("Get(file) after close = %#v, want closed error", value)
	}

	entries, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", tempDir, err)
	}
	if len(entries) == 0 {
		t.Fatalf("expected SlateDB files under %s, found none", tempDir)
	}
}
