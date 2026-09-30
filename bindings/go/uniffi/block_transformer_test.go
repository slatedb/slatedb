package slatedb_test

import (
	"errors"
	"fmt"
	"testing"

	slatedb "slatedb.io/slatedb-go/uniffi"
)

func valueOf(got *[]byte) string {
	if got == nil {
		return "<nil>"
	}
	return string(*got)
}

// flip inverts every byte: its own inverse, so a block it did not write
// decodes to bytes the block decoder refuses.
type flip struct{}

func (flip) Encode(data []byte) ([]byte, error) {
	out := make([]byte, len(data))
	for i, b := range data {
		out[i] = ^b
	}
	return out, nil
}

func (f flip) Decode(data []byte) ([]byte, error) { return f.Encode(data) }

// A database whose blocks go through a transform is readable only through
// the same transform: a plain reader fails at its replay or its first block,
// a reader carrying the transform reads the value back.
func TestBlocksWrittenThroughATransformReadBackOnlyThroughIt(t *testing.T) {
	store := newMemoryStore(t)
	dbHandle := openTestDB(t, store, func(t *testing.T, builder *slatedb.DbBuilder) {
		t.Helper()
		if err := builder.WithBlockTransformer(flip{}); err != nil {
			t.Fatalf("DbBuilder.WithBlockTransformer(): %v", err)
		}
	})
	if _, err := dbHandle.db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put(): %v", err)
	}
	if err := dbHandle.db.FlushWithOptions(slatedb.FlushOptions{FlushType: slatedb.FlushTypeMemTable}); err != nil {
		t.Fatalf("FlushWithOptions(): %v", err)
	}
	if got, err := dbHandle.db.Get([]byte("k")); err != nil || valueOf(got) != "v" {
		t.Fatalf("Get() through the writer = %q, %v", valueOf(got), err)
	}

	plain := slatedb.NewDbReaderBuilder(testDBPath, store)
	defer plain.Destroy()
	if err := plain.WithReaderMode(slatedb.ReaderModeFollowLatest{}); err != nil {
		t.Fatalf("WithReaderMode(): %v", err)
	}
	if reader, err := plain.Build(); err == nil {
		got, err := reader.Get([]byte("k"))
		if err == nil {
			t.Fatalf("a reader without the transform read %q from a transformed block", valueOf(got))
		}
		_ = reader.Shutdown()
		reader.Destroy()
	} else if !errors.Is(err, slatedb.ErrErrorData) {
		t.Fatalf("a reader without the transform failed with %v, want a data error", err)
	}

	flipped := openTestReader(t, store, func(t *testing.T, builder *slatedb.DbReaderBuilder) {
		t.Helper()
		if err := builder.WithReaderMode(slatedb.ReaderModeFollowLatest{}); err != nil {
			t.Fatalf("WithReaderMode(): %v", err)
		}
		if err := builder.WithBlockTransformer(flip{}); err != nil {
			t.Fatalf("DbReaderBuilder.WithBlockTransformer(): %v", err)
		}
	})
	if got, err := flipped.reader.Get([]byte("k")); err != nil || valueOf(got) != "v" {
		t.Fatalf("Get() through the transforming reader = %q, %v; want v", valueOf(got), err)
	}
}

// wrongKey decodes nothing: it returns a plain Go error, not a
// BlockTransformerCallbackError, as an application's decrypt failure would.
type wrongKey struct{}

func (wrongKey) Encode(data []byte) ([]byte, error) { return data, nil }

func (wrongKey) Decode([]byte) ([]byte, error) {
	return nil, fmt.Errorf("decrypt: wrong key")
}

// A transformer that fails with an arbitrary error makes the read a data
// error; it never panics the Rust side.
func TestATransformersArbitraryErrorIsADataError(t *testing.T) {
	store := newMemoryStore(t)
	dbHandle := openTestDB(t, store, nil)
	if _, err := dbHandle.db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put(): %v", err)
	}
	if err := dbHandle.db.FlushWithOptions(slatedb.FlushOptions{FlushType: slatedb.FlushTypeMemTable}); err != nil {
		t.Fatalf("FlushWithOptions(): %v", err)
	}

	builder := slatedb.NewDbReaderBuilder(testDBPath, store)
	defer builder.Destroy()
	if err := builder.WithReaderMode(slatedb.ReaderModeFollowLatest{}); err != nil {
		t.Fatalf("WithReaderMode(): %v", err)
	}
	if err := builder.WithBlockTransformer(wrongKey{}); err != nil {
		t.Fatalf("WithBlockTransformer(): %v", err)
	}
	reader, err := builder.Build()
	if err == nil {
		_, err = reader.Get([]byte("k"))
		_ = reader.Shutdown()
		reader.Destroy()
	}
	if err == nil {
		t.Fatal("a reader whose transformer fails read a value")
	}
	if !errors.Is(err, slatedb.ErrErrorData) {
		t.Fatalf("a failing transformer surfaced %v, want a data error", err)
	}
}
