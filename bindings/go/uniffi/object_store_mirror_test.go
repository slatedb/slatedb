package slatedb_test

import (
	"bytes"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	slatedb "slatedb.io/slatedb-go/uniffi"
)

func buildMirror(localDir string, store *slatedb.ObjectStore) (*slatedb.ObjectStore, error) {
	builder := slatedb.NewObjectStoreMirrorBuilder(localDir, store)
	defer builder.Destroy()
	return builder.Build()
}

func TestObjectStoreMirrorKeepsFlushedSstsLocal(t *testing.T) {
	localDir := t.TempDir()
	mirror, err := buildMirror(localDir, newMemoryStore(t))
	if err != nil {
		t.Fatalf("Build(): %v", err)
	}
	t.Cleanup(mirror.Destroy)

	handle := openTestDB(t, mirror, nil)
	if _, err := handle.db.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("Put(key): %v", err)
	}
	if err := handle.db.FlushWithOptions(slatedb.FlushOptions{FlushType: slatedb.FlushTypeMemTable}); err != nil {
		t.Fatalf("FlushWithOptions(MemTable): %v", err)
	}
	if err := handle.db.Shutdown(); err != nil {
		t.Fatalf("Shutdown(): %v", err)
	}
	handle.open = false

	objects, err := filepath.Glob(filepath.Join(localDir, "*.obj"))
	if err != nil {
		t.Fatalf("Glob(*.obj): %v", err)
	}
	if len(objects) == 0 {
		t.Fatalf("no local copies in %s", localDir)
	}

	// The same mirror serves each open of its database.
	reopened := openTestDB(t, mirror, nil)
	value, err := reopened.db.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Get(key): %v", err)
	}
	if value == nil || !bytes.Equal(*value, []byte("value")) {
		t.Fatalf("Get(key): got %v, want %q", value, "value")
	}
}

func TestObjectStoreMirrorRejectsLockedDirectory(t *testing.T) {
	localDir := t.TempDir()
	store := newMemoryStore(t)
	mirror, err := buildMirror(localDir, store)
	if err != nil {
		t.Fatalf("Build(): %v", err)
	}
	t.Cleanup(mirror.Destroy)

	second, err := buildMirror(localDir, store)
	if err == nil {
		second.Destroy()
	}
	if !errors.Is(err, slatedb.ErrErrorUnavailable) || !strings.Contains(err.Error(), "locked") {
		t.Fatalf("second mirror gave %v, want an unavailable error for a locked directory", err)
	}
}

type rejectingFilter struct {
	mu    sync.Mutex
	asked map[string]bool
}

func (f *rejectingFilter) ShouldMirror(segment []byte) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.asked[string(segment)] = true
	return false, nil
}

type failingFilter struct{}

func (failingFilter) ShouldMirror([]byte) (bool, error) {
	return false, errors.New("boom")
}

func assertNothingMirrored(t *testing.T, filter slatedb.SegmentFilter) {
	t.Helper()

	localDir := t.TempDir()
	builder := slatedb.NewObjectStoreMirrorBuilder(localDir, newMemoryStore(t))
	defer builder.Destroy()
	if err := builder.WithSegmentFilter(filter); err != nil {
		t.Fatalf("WithSegmentFilter(): %v", err)
	}
	mirror, err := builder.Build()
	if err != nil {
		t.Fatalf("Build(): %v", err)
	}
	t.Cleanup(mirror.Destroy)

	handle := openTestDB(t, mirror, nil)
	if _, err := handle.db.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("Put(key): %v", err)
	}
	if err := handle.db.FlushWithOptions(slatedb.FlushOptions{FlushType: slatedb.FlushTypeMemTable}); err != nil {
		t.Fatalf("FlushWithOptions(MemTable): %v", err)
	}
	value, err := handle.db.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Get(key): %v", err)
	}
	if value == nil || !bytes.Equal(*value, []byte("value")) {
		t.Fatalf("Get(key): got %v, want %q", value, "value")
	}

	objects, err := filepath.Glob(filepath.Join(localDir, "*.obj"))
	if err != nil {
		t.Fatalf("Glob(*.obj): %v", err)
	}
	if len(objects) != 0 {
		t.Fatalf("local copies %v, want none", objects)
	}
}

func TestObjectStoreMirrorSkipsSegmentsThatFilterRejects(t *testing.T) {
	filter := &rejectingFilter{asked: map[string]bool{}}
	assertNothingMirrored(t, filter)

	// A database without a segment extractor has only the root segment.
	filter.mu.Lock()
	defer filter.mu.Unlock()
	if len(filter.asked) != 1 || !filter.asked[""] {
		t.Fatalf("filter asked about %v, want only the root segment", filter.asked)
	}
}

func TestObjectStoreMirrorSkipsSegmentsWhenFilterFails(t *testing.T) {
	assertNothingMirrored(t, failingFilter{})
}
