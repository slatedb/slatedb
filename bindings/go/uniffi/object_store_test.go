package slatedb_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	slatedb "slatedb.io/slatedb-go/uniffi"
)

func TestObjectStoreRoundTrip(t *testing.T) {
	for _, backend := range []string{"memory", "local"} {
		t.Run(backend, func(t *testing.T) {
			var store *slatedb.ObjectStore
			prefix := "segments"
			if backend == "local" {
				store, prefix = newLocalStore(t)
				prefix += "/segments"
			} else {
				store = newMemoryStore(t)
			}
			location := prefix + "/a.parquet"
			want := []byte{0, 255, 1, 128}
			if err := store.Put(location, want); err != nil {
				t.Fatal(err)
			}
			got, err := store.Get(location)
			if err != nil || got == nil || !bytes.Equal(*got, want) {
				t.Fatalf("Get: %v, %v", got, err)
			}
			ranged, err := store.GetRange(location, 1, 3)
			if err != nil || ranged == nil || !bytes.Equal(*ranged, want[1:3]) {
				t.Fatalf("GetRange: %v, %v", ranged, err)
			}
			if backend == "local" {
				raw, err := os.ReadFile(filepath.Join("/", location))
				if err != nil || !bytes.Equal(raw, want) {
					t.Fatalf("ReadFile: %v, %v", raw, err)
				}
				want = []byte{3, 2, 1}
				if err := os.WriteFile(filepath.Join("/", location), want, 0600); err != nil {
					t.Fatal(err)
				}
				got, err = store.Get(location)
				if err != nil || got == nil || !bytes.Equal(*got, want) {
					t.Fatalf("Get external write: %v, %v", got, err)
				}
			}
			meta, err := store.Head(location)
			if err != nil || meta == nil || meta.Size != uint64(len(want)) || meta.Location != location {
				t.Fatalf("Head: %v, %v", meta, err)
			}
			iter, err := store.List(&prefix)
			if err != nil {
				t.Fatal(err)
			}
			defer iter.Destroy()
			listed, err := iter.Next()
			if err != nil || !reflect.DeepEqual(listed, meta) {
				t.Fatalf("List: %v, %v; Head: %v", listed, err, meta)
			}
			if err := store.Put(location, nil); err != nil {
				t.Fatal(err)
			}
			got, err = store.Get(location)
			if err != nil || got == nil || len(*got) != 0 {
				t.Fatalf("Get empty: %v, %v", got, err)
			}
			for i := 0; i < 2; i++ {
				if err := store.Delete(location); err != nil {
					t.Fatal(err)
				}
			}
			if got, err := store.Get(location); err != nil || got != nil {
				t.Fatalf("Get missing: %v, %v", got, err)
			}
			if got, err := store.GetRange(location, 0, 1); err != nil || got != nil {
				t.Fatalf("GetRange missing: %v, %v", got, err)
			}
			if got, err := store.Head(location); err != nil || got != nil {
				t.Fatalf("Head missing: %v, %v", got, err)
			}
		})
	}
}

func TestObjectStoreListSurvivesTheStoreHandle(t *testing.T) {
	store := newMemoryStore(t)
	for _, location := range []string{"segments/a", "segments/sub/b", "segments-other/c"} {
		if err := store.Put(location, []byte{1}); err != nil {
			t.Fatal(err)
		}
	}
	prefix := "segments"
	iter, err := store.List(&prefix)
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Destroy()
	store.Destroy()
	var locations []string
	for {
		meta, err := iter.Next()
		if err != nil {
			t.Fatal(err)
		}
		if meta == nil {
			break
		}
		locations = append(locations, meta.Location)
	}
	sort.Strings(locations)
	if !reflect.DeepEqual(locations, []string{"segments/a", "segments/sub/b"}) {
		t.Fatalf("List: %v", locations)
	}
	if meta, err := iter.Next(); err != nil || meta != nil {
		t.Fatalf("Next exhausted: %v, %v", meta, err)
	}
}

func TestObjectStoreRefusesInvalidPaths(t *testing.T) {
	store := newMemoryStore(t)
	location := "segments/../outside"
	assertInvalid := func(err error) {
		t.Helper()
		var invalid *slatedb.ErrorInvalid
		if !errors.As(err, &invalid) {
			t.Fatalf("Error: %v, want ErrorInvalid", err)
		}
	}
	assertInvalid(store.Put(location, nil))
	_, err := store.Get(location)
	assertInvalid(err)
	_, err = store.GetRange(location, 0, 1)
	assertInvalid(err)
	for _, bounds := range [][2]uint64{{2, 1}, {1, 1}} {
		_, err = store.GetRange("segments/a", bounds[0], bounds[1])
		assertInvalid(err)
	}
	_, err = store.Head(location)
	assertInvalid(err)
	assertInvalid(store.Delete(location))
	_, err = store.List(&location)
	assertInvalid(err)
	iter, err := store.List(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Destroy()
	if meta, err := iter.Next(); err != nil || meta != nil {
		t.Fatalf("List after refused puts: %v, %v", meta, err)
	}
}
