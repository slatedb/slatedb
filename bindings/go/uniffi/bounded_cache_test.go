package slatedb_test

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"

	slatedb "slatedb.io/slatedb-go/uniffi"
)

func newBoundedCache(t *testing.T, capacity uint64) *slatedb.DbCache {
	t.Helper()
	cache, err := slatedb.DbCacheNewBoundedFoyerCache(slatedb.FoyerCacheOptions{MaxCapacity: capacity, Shards: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cache.Destroy)
	return cache
}

func TestBoundedCacheOptionsAndObservation(t *testing.T) {
	for _, options := range []slatedb.FoyerCacheOptions{
		{MaxCapacity: 0, Shards: 1}, {MaxCapacity: 1, Shards: 0}, {MaxCapacity: 1, Shards: 2},
	} {
		cache, err := slatedb.DbCacheNewBoundedFoyerCache(options)
		if cache != nil {
			cache.Destroy()
		}
		if !errors.Is(err, slatedb.ErrErrorInvalid) {
			t.Fatalf("options %+v: got %v, want invalid", options, err)
		}
	}
	bounded := newBoundedCache(t, 64)
	if weight := bounded.IndexedWeight(); weight == nil || *weight != 0 {
		t.Fatalf("empty bounded cache: got %v, want supported zero", weight)
	}
	ordinary, err := slatedb.DbCacheNewFoyerCache(slatedb.FoyerCacheOptions{MaxCapacity: 64, Shards: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ordinary.Destroy)
	moka, err := slatedb.DbCacheNewMokaCache(slatedb.MokaCacheOptions{MaxCapacity: 64})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(moka.Destroy)
	split, err := slatedb.DbCacheNewSplitCache(bounded, bounded)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(split.Destroy)
	for name, cache := range map[string]*slatedb.DbCache{"ordinary": ordinary, "moka": moka, "split": split} {
		if weight := cache.IndexedWeight(); weight != nil {
			t.Fatalf("%s: got supported weight %d, want unsupported", name, *weight)
		}
	}
}

const cacheFixtureKeys = 96

func cacheFixtureKey(i int) []byte   { return []byte(fmt.Sprintf("key-%04d", i)) }
func cacheFixtureValue(i int) []byte { return bytes.Repeat([]byte{byte(i)}, 1024) }

func seedCacheFixture(t *testing.T, store *slatedb.ObjectStore) {
	t.Helper()
	writer := openTestDB(t, store, func(t *testing.T, builder *slatedb.DbBuilder) {
		if err := builder.WithDbCacheDisabled(); err != nil {
			t.Fatal(err)
		}
	})
	for i := 0; i < cacheFixtureKeys; i++ {
		write, err := writer.db.Put(cacheFixtureKey(i), cacheFixtureValue(i))
		if err != nil {
			t.Fatal(err)
		}
		write.Destroy()
	}
	if err := writer.db.FlushWithOptions(slatedb.FlushOptions{FlushType: slatedb.FlushTypeMemTable}); err != nil {
		t.Fatal(err)
	}
	if err := writer.db.Shutdown(); err != nil {
		t.Fatal(err)
	}
	writer.open = false
}

type cacheFixtureGetter interface {
	Get([]byte) (*[]byte, error)
}

func checkCacheFixture(getter cacheFixtureGetter, i int) error {
	value, err := getter.Get(cacheFixtureKey(i))
	if err != nil {
		return err
	}
	if value == nil || !bytes.Equal(*value, cacheFixtureValue(i)) {
		return fmt.Errorf("key %d: unexpected value %v", i, value)
	}
	return nil
}

func boundedWeight(t *testing.T, cache *slatedb.DbCache, capacity uint64) uint64 {
	t.Helper()
	weight := cache.IndexedWeight()
	if weight == nil || *weight > capacity {
		t.Fatalf("indexed weight %v exceeds capacity %d or is unsupported", weight, capacity)
	}
	return *weight
}

func TestBoundedCacheSharedWriterAndTrueReader(t *testing.T) {
	for _, dataCapacity := range []uint64{256 * 1024, 32 * 1024} {
		t.Run(fmt.Sprintf("data_capacity_%d", dataCapacity), func(t *testing.T) {
			store := newMemoryStore(t)
			seedCacheFixture(t, store)
			const metaCapacity = uint64(8 * 1024)
			data, meta := newBoundedCache(t, dataCapacity), newBoundedCache(t, metaCapacity)
			cache, err := slatedb.DbCacheNewSplitCache(data, meta)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(cache.Destroy)
			// Opening/replaying a handle can populate the cache before its first Get.
			before := boundedWeight(t, data, dataCapacity)
			writer := openTestDB(t, store, func(t *testing.T, builder *slatedb.DbBuilder) {
				if err := builder.WithDbCache(cache, 1); err != nil {
					t.Fatal(err)
				}
			})
			if err := checkCacheFixture(writer.db, 0); err != nil {
				t.Fatal(err)
			}
			afterWriter := boundedWeight(t, data, dataCapacity)
			if afterWriter <= before {
				t.Fatalf("writer did not populate configured data cache: before %d, after %d", before, afterWriter)
			}
			reader := openTestReader(t, store, func(t *testing.T, builder *slatedb.DbReaderBuilder) {
				if err := builder.WithReaderMode(slatedb.ReaderModeFollowLatest{}); err != nil {
					t.Fatal(err)
				}
				if err := builder.WithDbCache(cache, 2); err != nil {
					t.Fatal(err)
				}
			})
			if err := checkCacheFixture(reader.reader, 0); err != nil {
				t.Fatal(err)
			}
			if afterReader := boundedWeight(t, data, dataCapacity); dataCapacity == 256*1024 && afterReader <= afterWriter {
				t.Fatalf("true reader did not populate its distinct scope in the shared data cache: writer %d, reader %d", afterWriter, afterReader)
			}
			// The 32 KiB case forces both handles to read more than their shared
			// budget. The 256 KiB case has room to observe both scoped contributions.
			// All workers join before cleanup shuts down either native handle.
			var workers sync.WaitGroup
			failures := make(chan error, 2)
			for _, getter := range []cacheFixtureGetter{writer.db, reader.reader} {
				workers.Add(1)
				go func(getter cacheFixtureGetter) {
					defer workers.Done()
					for i := 0; i < cacheFixtureKeys; i++ {
						if err := checkCacheFixture(getter, i); err != nil {
							failures <- err
							return
						}
						for _, budget := range []struct {
							cache    *slatedb.DbCache
							capacity uint64
						}{{data, dataCapacity}, {meta, metaCapacity}} {
							if weight := budget.cache.IndexedWeight(); weight == nil || *weight > budget.capacity {
								failures <- fmt.Errorf("unsupported or excessive indexed weight %v, capacity %d", weight, budget.capacity)
								return
							}
						}
					}
				}(getter)
			}
			workers.Wait()
			close(failures)
			for err := range failures {
				t.Error(err)
			}
			if boundedWeight(t, data, dataCapacity) == 0 || boundedWeight(t, meta, metaCapacity) == 0 {
				t.Fatal("shared split cache did not retain both data and metadata")
			}
		})
	}
}

func TestBoundedCacheDisabledWriterAndTrueReader(t *testing.T) {
	store := newMemoryStore(t)
	seedCacheFixture(t, store)
	cache := newBoundedCache(t, 1024*1024)
	writer := openTestDB(t, store, func(t *testing.T, builder *slatedb.DbBuilder) {
		if err := builder.WithDbCache(cache, 1); err != nil {
			t.Fatal(err)
		}
		if err := builder.WithDbCacheDisabled(); err != nil {
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
		if err := builder.WithDbCacheDisabled(); err != nil {
			t.Fatal(err)
		}
	})
	for _, getter := range []cacheFixtureGetter{writer.db, reader.reader} {
		for i := 0; i < cacheFixtureKeys; i++ {
			if err := checkCacheFixture(getter, i); err != nil {
				t.Fatal(err)
			}
		}
	}
	if weight := boundedWeight(t, cache, 1024*1024); weight != 0 {
		t.Fatalf("disabled handles populated the previously configured cache: %d", weight)
	}
}

func TestBoundedCacheOwnerReleasedBeforeBuild(t *testing.T) {
	store := newMemoryStore(t)
	seedCacheFixture(t, store)
	cache, err := slatedb.DbCacheNewBoundedFoyerCache(slatedb.FoyerCacheOptions{MaxCapacity: 32 * 1024, Shards: 1})
	if err != nil {
		t.Fatal(err)
	}
	writerBuilder := slatedb.NewDbBuilder(testDBPath, store)
	defer writerBuilder.Destroy()
	readerBuilder := slatedb.NewDbReaderBuilder(testDBPath, store)
	defer readerBuilder.Destroy()
	if err := writerBuilder.WithDbCache(cache, 1); err != nil {
		cache.Destroy()
		t.Fatal(err)
	}
	if err := readerBuilder.WithDbCache(cache, 2); err != nil {
		cache.Destroy()
		t.Fatal(err)
	}
	cache.Destroy()
	if err := readerBuilder.WithReaderMode(slatedb.ReaderModeFollowLatest{}); err != nil {
		t.Fatal(err)
	}
	writer, err := writerBuilder.Build()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := writer.Shutdown(); err != nil {
			t.Error(err)
		}
		writer.Destroy()
	})
	reader, err := readerBuilder.Build()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reader.Shutdown(); err != nil {
			t.Error(err)
		}
		reader.Destroy()
	})
	for _, getter := range []cacheFixtureGetter{writer, reader} {
		for i := 0; i < cacheFixtureKeys; i++ {
			if err := checkCacheFixture(getter, i); err != nil {
				t.Fatal(err)
			}
		}
	}
}
