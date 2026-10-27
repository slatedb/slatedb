package slatedb_test

import (
	"testing"

	slatedb "slatedb.io/slatedb-go/uniffi"
)

// The binding must select the bounded implementation even for valid options.
// Every fixture data block exceeds this positive one-byte data budget.
func TestBoundedCacheOversizedValuesRemainUnindexed(t *testing.T) {
	store := newMemoryStore(t)
	seedCacheFixture(t, store)
	data, meta := newBoundedCache(t, 1), newBoundedCache(t, 8*1024)
	cache, err := slatedb.DbCacheNewSplitCache(data, meta)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cache.Destroy)
	writer := openTestDB(t, store, func(t *testing.T, builder *slatedb.DbBuilder) {
		if err := builder.WithDbCache(cache, 1); err != nil {
			t.Fatal(err)
		}
	})
	reader := openTestReader(t, store, func(t *testing.T, builder *slatedb.DbReaderBuilder) {
		if err := builder.WithReaderMode(slatedb.ReaderModeFollowLatest{}); err != nil {
			t.Fatal(err)
		}
		if err := builder.WithDbCache(cache, 1); err != nil {
			t.Fatal(err)
		}
	})
	for _, getter := range []cacheFixtureGetter{writer.db, reader.reader} {
		for i := 0; i < cacheFixtureKeys; i++ {
			if err := checkCacheFixture(getter, i); err != nil {
				t.Fatal(err)
			}
			if weight := data.IndexedWeight(); weight == nil || *weight != 0 {
				t.Fatalf("oversized data was indexed: %v, want supported zero", weight)
			}
		}
	}
}
