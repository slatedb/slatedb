package slatedb_test

import (
	"bytes"
	"fmt"
	"testing"

	slatedb "slatedb.io/slatedb-go/uniffi"
)

var (
	benchBatchSizes = []int{64, 1024}
	benchValueSizes = []int{11, 1 << 10, 16 << 10}
)

func benchRecords(n, valueSize int) ([][]byte, [][]byte) {
	keys := make([][]byte, n)
	values := make([][]byte, n)
	for i := range n {
		keys[i] = fmt.Appendf(nil, "key-%08d", i)
		values[i] = bytes.Repeat([]byte{byte('a' + i%26)}, valueSize)
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

func fillExtend(b *testing.B, batch *slatedb.WriteBatch, keys, values [][]byte) {
	ops := make([]slatedb.WriteOp, len(keys))
	for i := range keys {
		ops[i] = slatedb.WriteOpPut{Key: keys[i], Val: values[i]}
	}
	if err := batch.Extend(ops); err != nil {
		b.Fatalf("Extend: %v", err)
	}
}

type fillFunc func(*testing.B, *slatedb.WriteBatch, [][]byte, [][]byte)

var fills = []struct {
	name string
	fill fillFunc
}{
	{"Put", fillPutLoop},
	{"Extend", fillExtend},
}

func reportEntries(b *testing.B, keys, values [][]byte) {
	var n int64
	for i := range keys {
		n += int64(len(keys[i]) + len(values[i]))
	}
	b.SetBytes(n)
	b.ReportMetric(float64(len(keys))*float64(b.N)/b.Elapsed().Seconds(), "entries/s")
}

func BenchmarkWriteBatchFill(b *testing.B) {
	for _, size := range benchBatchSizes {
		for _, valueSize := range benchValueSizes {
			keys, values := benchRecords(size, valueSize)
			for _, tc := range fills {
				b.Run(fmt.Sprintf("%s/n=%d/v=%d", tc.name, size, valueSize), func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						batch := slatedb.NewWriteBatch()
						tc.fill(b, batch, keys, values)
						batch.Destroy()
					}
					reportEntries(b, keys, values)
				})
			}
		}
	}
}

func BenchmarkWriteBatchCommit(b *testing.B) {
	for _, size := range benchBatchSizes {
		for _, valueSize := range benchValueSizes {
			keys, values := benchRecords(size, valueSize)
			for _, tc := range fills {
				b.Run(fmt.Sprintf("%s/n=%d/v=%d", tc.name, size, valueSize), func(b *testing.B) {
					db := openEmptyBenchDB(b)
					b.ReportAllocs()
					var last uint64
					for b.Loop() {
						batch := slatedb.NewWriteBatch()
						tc.fill(b, batch, keys, values)
						handle, err := db.WriteWithOptions(batch, slatedb.WriteOptions{})
						if err != nil {
							b.Fatalf("WriteWithOptions: %v", err)
						}
						seq := handle.Seqnum()
						handle.Destroy()
						batch.Destroy()
						if seq <= last {
							b.Fatalf("seqnum %d not after %d", seq, last)
						}
						last = seq
					}
					reportEntries(b, keys, values)
					value, err := db.Get(keys[size-1])
					if err != nil || value == nil {
						b.Fatalf("Get after commits: value=%v err=%v", value, err)
					}
				})
			}
		}
	}
}
