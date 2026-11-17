package slatedb_test

import (
	"fmt"
	"testing"

	slatedb "slatedb.io/slatedb-go/uniffi"
)

const benchBatchSize = 64

func benchRecords(n int) ([][]byte, [][]byte) {
	keys := make([][]byte, n)
	values := make([][]byte, n)
	for i := range n {
		keys[i] = fmt.Appendf(nil, "key-%08d", i)
		values[i] = []byte("value-01234")
	}
	return keys, values
}

func openEmptyBenchDB(b *testing.B) *slatedb.Db {
	b.Helper()
	store, err := slatedb.ObjectStoreResolve("memory:///")
	if err != nil {
		b.Fatalf("ObjectStoreResolve: %v", err)
	}
	builder := slatedb.NewDbBuilder("bench", store)
	db, err := builder.Build()
	builder.Destroy()
	if err != nil {
		b.Fatalf("Build: %v", err)
	}
	b.Cleanup(func() {
		if err := db.Shutdown(); err != nil {
			b.Errorf("Shutdown: %v", err)
		}
		db.Destroy()
		store.Destroy()
	})
	return db
}

func fillPutLoop(b *testing.B, batch *slatedb.WriteBatch, keys, values [][]byte) {
	for i := range keys {
		if err := batch.Put(keys[i], values[i]); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
}

func fillPutMany(b *testing.B, batch *slatedb.WriteBatch, keys, values [][]byte) {
	entries := make([]slatedb.PutEntry, len(keys))
	for i := range keys {
		entries[i] = slatedb.PutEntry{Key: keys[i], Value: values[i]}
	}
	if err := batch.PutMany(entries); err != nil {
		b.Fatalf("PutMany: %v", err)
	}
}

func BenchmarkWriteBatchFill(b *testing.B) {
	keys, values := benchRecords(benchBatchSize)
	for _, tc := range []struct {
		name string
		fill func(*testing.B, *slatedb.WriteBatch, [][]byte, [][]byte)
	}{
		{"Put", fillPutLoop},
		{"PutMany", fillPutMany},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				batch := slatedb.NewWriteBatch()
				tc.fill(b, batch, keys, values)
				batch.Destroy()
			}
		})
	}
}

func BenchmarkWriteBatchCommit(b *testing.B) {
	keys, values := benchRecords(benchBatchSize)
	for _, tc := range []struct {
		name   string
		commit func(*testing.B, *slatedb.Db) uint64
	}{
		{"Put+WriteWithOptions", func(b *testing.B, db *slatedb.Db) uint64 {
			batch := slatedb.NewWriteBatch()
			defer batch.Destroy()
			fillPutLoop(b, batch, keys, values)
			handle, err := db.WriteWithOptions(batch, slatedb.WriteOptions{})
			if err != nil {
				b.Fatalf("WriteWithOptions: %v", err)
			}
			defer handle.Destroy()
			return handle.Seqnum()
		}},
		{"PutMany+WriteWithOptions", func(b *testing.B, db *slatedb.Db) uint64 {
			batch := slatedb.NewWriteBatch()
			defer batch.Destroy()
			fillPutMany(b, batch, keys, values)
			handle, err := db.WriteWithOptions(batch, slatedb.WriteOptions{})
			if err != nil {
				b.Fatalf("WriteWithOptions: %v", err)
			}
			defer handle.Destroy()
			return handle.Seqnum()
		}},
		{"PutMany+WriteWithOptionsResult", func(b *testing.B, db *slatedb.Db) uint64 {
			batch := slatedb.NewWriteBatch()
			defer batch.Destroy()
			fillPutMany(b, batch, keys, values)
			result, err := db.WriteWithOptionsResult(batch, slatedb.WriteOptions{})
			if err != nil {
				b.Fatalf("WriteWithOptionsResult: %v", err)
			}
			return result.Seqnum
		}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			db := openEmptyBenchDB(b)
			b.ReportAllocs()
			var commits, last uint64
			for b.Loop() {
				seq := tc.commit(b, db)
				if seq <= last {
					b.Fatalf("seqnum %d not after %d", seq, last)
				}
				last = seq
				commits++
			}
			value, err := db.Get(keys[benchBatchSize-1])
			if err != nil || value == nil {
				b.Fatalf("Get after %d commits: value=%v err=%v", commits, value, err)
			}
		})
	}
}
