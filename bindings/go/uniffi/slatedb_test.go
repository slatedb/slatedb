package slatedb_test

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	slatedb "slatedb.io/slatedb-go/uniffi"
)

const testDBPath = "test-db"

const (
	dbRequestCountMetricName = "slatedb.db.request_count"
	dbWriteOpsMetricName     = "slatedb.db.write_ops"
)

type testDB struct {
	db   *slatedb.Db
	open bool
}

type testReader struct {
	reader *slatedb.DbReader
	open   bool
}

func openTestAdmin(t *testing.T, store *slatedb.ObjectStore, configure func(*testing.T, *slatedb.AdminBuilder)) *slatedb.Admin {
	t.Helper()

	builder := slatedb.NewAdminBuilder(testDBPath, store)
	defer builder.Destroy()

	if configure != nil {
		configure(t, builder)
	}

	admin, err := builder.Build()
	if err != nil {
		t.Fatalf("AdminBuilder.Build(): %v", err)
	}

	t.Cleanup(admin.Destroy)
	return admin
}

func newMemoryStore(t *testing.T) *slatedb.ObjectStore {
	t.Helper()

	store, err := slatedb.ObjectStoreResolve("memory:///")
	if err != nil {
		t.Fatalf("ObjectStoreResolve(memory:///): %v", err)
	}
	t.Cleanup(store.Destroy)
	return store
}

func openTestDB(t *testing.T, store *slatedb.ObjectStore, configure func(*testing.T, *slatedb.DbBuilder)) *testDB {
	t.Helper()

	builder := slatedb.NewDbBuilder(testDBPath, store)
	defer builder.Destroy()

	if configure != nil {
		configure(t, builder)
	}

	db, err := builder.Build()
	if err != nil {
		t.Fatalf("Build(): %v", err)
	}

	handle := &testDB{db: db, open: true}
	t.Cleanup(func() {
		if handle.db == nil {
			return
		}
		if handle.open {
			if err := handle.db.Shutdown(); err != nil {
				t.Errorf("Shutdown(): %v", err)
			}
		}
		handle.db.Destroy()
	})

	return handle
}

func openTestReader(t *testing.T, store *slatedb.ObjectStore, configure func(*testing.T, *slatedb.DbReaderBuilder)) *testReader {
	t.Helper()

	builder := slatedb.NewDbReaderBuilder(testDBPath, store)
	defer builder.Destroy()

	if configure != nil {
		configure(t, builder)
	}

	reader, err := builder.Build()
	if err != nil {
		t.Fatalf("DbReaderBuilder.Build(): %v", err)
	}

	handle := &testReader{reader: reader, open: true}
	t.Cleanup(func() {
		if handle.reader == nil {
			return
		}
		if handle.open {
			if err := handle.reader.Shutdown(); err != nil {
				t.Errorf("DbReader.Shutdown(): %v", err)
			}
		}
		handle.reader.Destroy()
	})

	return handle
}

func openTestWalReader(t *testing.T, store *slatedb.ObjectStore) *slatedb.WalReader {
	t.Helper()

	reader := slatedb.NewWalReader(testDBPath, store)
	if reader == nil {
		t.Fatal("NewWalReader(): got nil reader")
	}

	t.Cleanup(reader.Destroy)
	return reader
}

func bytesPtr(b []byte) *[]byte {
	clone := append([]byte(nil), b...)
	return &clone
}

func uint64Ptr(v uint64) *uint64 {
	return &v
}

func drainIterator(t *testing.T, iter *slatedb.DbIterator) []slatedb.KeyValue {
	t.Helper()

	var rows []slatedb.KeyValue
	for {
		row, err := iter.Next()
		if err != nil {
			t.Fatalf("iterator Next(): %v", err)
		}
		if row == nil {
			return rows
		}
		rows = append(rows, *row)
	}
}

func drainWalIterator(t *testing.T, iter *slatedb.WalFileIterator) []slatedb.RowEntry {
	t.Helper()

	var rows []slatedb.RowEntry
	for {
		row, err := iter.Next()
		if err != nil {
			t.Fatalf("wal iterator Next(): %v", err)
		}
		if row == nil {
			return rows
		}
		rows = append(rows, *row)
	}
}

func requireRows(t *testing.T, got []slatedb.KeyValue, wantKeys []string, wantValues []string) {
	t.Helper()

	if len(got) != len(wantKeys) || len(got) != len(wantValues) {
		t.Fatalf("row expectation mismatch: got=%d wantKeys=%d wantValues=%d", len(got), len(wantKeys), len(wantValues))
	}

	for i, row := range got {
		if string(row.Key) != wantKeys[i] {
			t.Fatalf("row %d key mismatch: got=%q want=%q", i, row.Key, wantKeys[i])
		}
		if !bytes.Equal(row.Value, []byte(wantValues[i])) {
			t.Fatalf("row %d value mismatch: got=%q want=%q", i, row.Value, wantValues[i])
		}
	}
}

func waitUntil(t *testing.T, timeout time.Duration, step time.Duration, check func() (bool, error)) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var lastErr error

	for {
		ok, err := check()
		if err == nil && ok {
			return
		}
		lastErr = err

		if time.Now().After(deadline) {
			if lastErr != nil {
				t.Fatalf("timed out after %s: %v", timeout, lastErr)
			}
			t.Fatalf("timed out after %s", timeout)
		}

		time.Sleep(step)
	}
}

type logCollector struct {
	mu      sync.Mutex
	records []slatedb.LogRecord
}

func (c *logCollector) Log(record slatedb.LogRecord) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.records = append(c.records, cloneLogRecord(record))
}

func (c *logCollector) matchingRecord(predicate func(slatedb.LogRecord) bool) (slatedb.LogRecord, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, record := range c.records {
		if predicate(record) {
			return cloneLogRecord(record), true
		}
	}

	return slatedb.LogRecord{}, false
}

func cloneLogRecord(record slatedb.LogRecord) slatedb.LogRecord {
	return slatedb.LogRecord{
		Level:      record.Level,
		Target:     record.Target,
		Message:    record.Message,
		ModulePath: cloneStringPtr(record.ModulePath),
		File:       cloneStringPtr(record.File),
		Line:       cloneUint32Ptr(record.Line),
	}
}

func cloneStringPtr(value *string) *string {
	if value == nil {
		return nil
	}

	cloned := *value
	return &cloned
}

func cloneUint32Ptr(value *uint32) *uint32 {
	if value == nil {
		return nil
	}

	cloned := *value
	return &cloned
}

type testMetricsRecorder struct {
	mu              sync.Mutex
	counters        map[string]uint64
	gauges          map[string]int64
	upDownCounters  map[string]int64
	histogramValues map[string][]float64
}

func newTestMetricsRecorder() *testMetricsRecorder {
	return &testMetricsRecorder{
		counters:        make(map[string]uint64),
		gauges:          make(map[string]int64),
		upDownCounters:  make(map[string]int64),
		histogramValues: make(map[string][]float64),
	}
}

func (r *testMetricsRecorder) RegisterCounter(name string, _ string, labels []slatedb.MetricLabel) slatedb.Counter {
	key := metricKey(name, labels)

	r.mu.Lock()
	r.counters[key] = 0
	r.mu.Unlock()

	return testCounterHandle(func(value uint64) {
		r.mu.Lock()
		r.counters[key] += value
		r.mu.Unlock()
	})
}

func (r *testMetricsRecorder) RegisterGauge(name string, _ string, labels []slatedb.MetricLabel) slatedb.Gauge {
	key := metricKey(name, labels)

	r.mu.Lock()
	r.gauges[key] = 0
	r.mu.Unlock()

	return testGaugeHandle(func(value int64) {
		r.mu.Lock()
		r.gauges[key] = value
		r.mu.Unlock()
	})
}

func (r *testMetricsRecorder) RegisterUpDownCounter(name string, _ string, labels []slatedb.MetricLabel) slatedb.UpDownCounter {
	key := metricKey(name, labels)

	r.mu.Lock()
	r.upDownCounters[key] = 0
	r.mu.Unlock()

	return testUpDownCounterHandle(func(value int64) {
		r.mu.Lock()
		r.upDownCounters[key] += value
		r.mu.Unlock()
	})
}

func (r *testMetricsRecorder) RegisterHistogram(name string, _ string, labels []slatedb.MetricLabel, _ []float64) slatedb.Histogram {
	key := metricKey(name, labels)

	r.mu.Lock()
	r.histogramValues[key] = nil
	r.mu.Unlock()

	return testHistogramHandle(func(value float64) {
		r.mu.Lock()
		r.histogramValues[key] = append(r.histogramValues[key], value)
		r.mu.Unlock()
	})
}

func (r *testMetricsRecorder) counterValue(name string, labels []slatedb.MetricLabel) (uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	value, ok := r.counters[metricKey(name, labels)]
	return value, ok
}

type testCounterHandle func(uint64)

func (h testCounterHandle) Increment(value uint64) {
	h(value)
}

type testGaugeHandle func(int64)

func (h testGaugeHandle) Set(value int64) {
	h(value)
}

type testUpDownCounterHandle func(int64)

func (h testUpDownCounterHandle) Increment(value int64) {
	h(value)
}

type testHistogramHandle func(float64)

func (h testHistogramHandle) Record(value float64) {
	h(value)
}

func metricKey(name string, labels []slatedb.MetricLabel) string {
	if len(labels) == 0 {
		return name
	}

	parts := make([]string, 0, len(labels))
	for _, label := range labels {
		parts = append(parts, label.Key+"="+label.Value)
	}
	return name + "|" + strings.Join(parts, ",")
}

type concatMergeOperator struct{}

func (concatMergeOperator) Merge(_ []byte, existingValue *[]byte, operand []byte) ([]byte, error) {
	size := len(operand)
	if existingValue != nil {
		size += len(*existingValue)
	}

	merged := make([]byte, 0, size)
	if existingValue != nil {
		merged = append(merged, (*existingValue)...)
	}
	merged = append(merged, operand...)
	return merged, nil
}

func seedWalFiles(t *testing.T, store *slatedb.ObjectStore) {
	t.Helper()

	handle := openTestDB(t, store, func(t *testing.T, builder *slatedb.DbBuilder) {
		t.Helper()
		if err := builder.WithMergeOperator(concatMergeOperator{}); err != nil {
			t.Fatalf("WithMergeOperator(): %v", err)
		}
	})

	if _, err := handle.db.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("Put(a): %v", err)
	}
	if _, err := handle.db.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("Put(b): %v", err)
	}
	if err := handle.db.FlushWithOptions(slatedb.FlushOptions{FlushType: slatedb.FlushTypeWal}); err != nil {
		t.Fatalf("FlushWithOptions(Wal) for value rows: %v", err)
	}

	if _, err := handle.db.Delete([]byte("a")); err != nil {
		t.Fatalf("Delete(a): %v", err)
	}
	if err := handle.db.FlushWithOptions(slatedb.FlushOptions{FlushType: slatedb.FlushTypeWal}); err != nil {
		t.Fatalf("FlushWithOptions(Wal) for tombstone row: %v", err)
	}

	if _, err := handle.db.Merge([]byte("m"), []byte("x")); err != nil {
		t.Fatalf("Merge(m): %v", err)
	}
	if err := handle.db.FlushWithOptions(slatedb.FlushOptions{FlushType: slatedb.FlushTypeWal}); err != nil {
		t.Fatalf("FlushWithOptions(Wal) for merge row: %v", err)
	}
}

func TestDbLifecycleAndStatus(t *testing.T) {
	store := newMemoryStore(t)
	handle := openTestDB(t, store, func(t *testing.T, builder *slatedb.DbBuilder) {
		t.Helper()
		blockCache, err := slatedb.DbCacheNewMokaCache(slatedb.MokaCacheOptions{MaxCapacity: 128 * 1024 * 1024})
		if err != nil {
			t.Fatalf("NewMokaCache: %v", err)
		}
		metaCache, err := slatedb.DbCacheNewFoyerCache(slatedb.FoyerCacheOptions{MaxCapacity: 256 * 1024 * 1024})
		if err != nil {
			t.Fatalf("NewFoyerCache: %v", err)
		}
		dbCache, err := slatedb.DbCacheNewSplitCache(blockCache, metaCache)
		if err != nil {
			t.Fatalf("NewSplitCache: %v", err)
		}
		if err := builder.WithDbCache(dbCache); err != nil {
			t.Fatalf("WithMergeOperator(): %v", err)
		}
	})

	status := handle.db.Status()
	if status.CloseReason != nil {
		t.Fatalf("Status() on open db: got close reason %v, want nil", *status.CloseReason)
	}

	if _, err := handle.db.Put([]byte("lifecycle"), []byte("value")); err != nil {
		t.Fatalf("Put(): %v", err)
	}

	if err := handle.db.Shutdown(); err != nil {
		t.Fatalf("Shutdown(): %v", err)
	}
	handle.open = false

	status = handle.db.Status()
	if status.CloseReason == nil {
		t.Fatalf("Status() after Shutdown(): got nil close reason, want %v", slatedb.CloseReasonClean)
	}
	if *status.CloseReason != slatedb.CloseReasonClean {
		t.Fatalf("Status() after Shutdown(): got close reason %v, want %v", *status.CloseReason, slatedb.CloseReasonClean)
	}

	if _, err := handle.db.Put([]byte("after-shutdown"), []byte("value")); !errors.Is(err, slatedb.ErrErrorClosed) {
		t.Fatalf("Put() after Shutdown(): got %v, want closed error", err)
	}
}

func TestDbCrudAndMetadata(t *testing.T) {
	store := newMemoryStore(t)
	handle := openTestDB(t, store, nil)

	readOptions := slatedb.ReadOptions{
		DurabilityFilter: slatedb.DurabilityLevelMemory,
		Dirty:            false,
		CacheBlocks:      true,
	}

	putOptions := slatedb.PutOptions{Ttl: slatedb.TtlDefault{}}
	writeOptions := slatedb.WriteOptions{AwaitDurable: true}

	firstWrite, err := handle.db.Put([]byte("alpha"), []byte("one"))
	if err != nil {
		t.Fatalf("Put(alpha): %v", err)
	}
	if firstWrite.Seqnum == 0 {
		t.Fatalf("Put(alpha): Seqnum = 0")
	}
	if firstWrite.CreateTs == 0 {
		t.Fatalf("Put(alpha): CreateTs = 0")
	}

	value, err := handle.db.Get([]byte("alpha"))
	if err != nil {
		t.Fatalf("Get(alpha): %v", err)
	}
	if value == nil || !bytes.Equal(*value, []byte("one")) {
		t.Fatalf("Get(alpha): got %v, want %q", value, "one")
	}

	value, err = handle.db.GetWithOptions([]byte("alpha"), readOptions)
	if err != nil {
		t.Fatalf("GetWithOptions(alpha): %v", err)
	}
	if value == nil || !bytes.Equal(*value, []byte("one")) {
		t.Fatalf("GetWithOptions(alpha): got %v, want %q", value, "one")
	}

	metadata, err := handle.db.GetKeyValue([]byte("alpha"))
	if err != nil {
		t.Fatalf("GetKeyValue(alpha): %v", err)
	}
	if metadata == nil {
		t.Fatal("GetKeyValue(alpha): got nil metadata")
	}
	if !bytes.Equal(metadata.Key, []byte("alpha")) {
		t.Fatalf("GetKeyValue(alpha): key = %q, want %q", metadata.Key, "alpha")
	}
	if !bytes.Equal(metadata.Value, []byte("one")) {
		t.Fatalf("GetKeyValue(alpha): value = %q, want %q", metadata.Value, "one")
	}
	if metadata.Seq != firstWrite.Seqnum {
		t.Fatalf("GetKeyValue(alpha): seq = %d, want %d", metadata.Seq, firstWrite.Seqnum)
	}
	if metadata.CreateTs != firstWrite.CreateTs {
		t.Fatalf("GetKeyValue(alpha): create ts = %d, want %d", metadata.CreateTs, firstWrite.CreateTs)
	}

	metadata, err = handle.db.GetKeyValueWithOptions([]byte("alpha"), readOptions)
	if err != nil {
		t.Fatalf("GetKeyValueWithOptions(alpha): %v", err)
	}
	if metadata == nil || !bytes.Equal(metadata.Value, []byte("one")) {
		t.Fatalf("GetKeyValueWithOptions(alpha): got %v, want value %q", metadata, "one")
	}

	secondWrite, err := handle.db.PutWithOptions([]byte("beta"), []byte("two"), putOptions, writeOptions)
	if err != nil {
		t.Fatalf("PutWithOptions(beta): %v", err)
	}
	if secondWrite.Seqnum <= firstWrite.Seqnum {
		t.Fatalf("PutWithOptions(beta): seq = %d, want > %d", secondWrite.Seqnum, firstWrite.Seqnum)
	}
	if secondWrite.CreateTs == 0 {
		t.Fatalf("PutWithOptions(beta): CreateTs = 0")
	}

	value, err = handle.db.Get([]byte("beta"))
	if err != nil {
		t.Fatalf("Get(beta): %v", err)
	}
	if value == nil || !bytes.Equal(*value, []byte("two")) {
		t.Fatalf("Get(beta): got %v, want %q", value, "two")
	}

	if _, err := handle.db.Put([]byte("empty"), []byte{}); err != nil {
		t.Fatalf("Put(empty): %v", err)
	}
	value, err = handle.db.Get([]byte("empty"))
	if err != nil {
		t.Fatalf("Get(empty): %v", err)
	}
	if value == nil || len(*value) != 0 {
		t.Fatalf("Get(empty): got %v, want empty slice", value)
	}

	value, err = handle.db.Get([]byte("missing"))
	if err != nil {
		t.Fatalf("Get(missing): %v", err)
	}
	if value != nil {
		t.Fatalf("Get(missing): got %q, want nil", *value)
	}

	deleteWrite, err := handle.db.Delete([]byte("alpha"))
	if err != nil {
		t.Fatalf("Delete(alpha): %v", err)
	}
	if deleteWrite.Seqnum <= secondWrite.Seqnum {
		t.Fatalf("Delete(alpha): seq = %d, want > %d", deleteWrite.Seqnum, secondWrite.Seqnum)
	}

	value, err = handle.db.Get([]byte("alpha"))
	if err != nil {
		t.Fatalf("Get(alpha) after delete: %v", err)
	}
	if value != nil {
		t.Fatalf("Get(alpha) after delete: got %q, want nil", *value)
	}

	deleteWrite, err = handle.db.DeleteWithOptions([]byte("beta"), writeOptions)
	if err != nil {
		t.Fatalf("DeleteWithOptions(beta): %v", err)
	}
	if deleteWrite.Seqnum <= secondWrite.Seqnum {
		t.Fatalf("DeleteWithOptions(beta): seq = %d, want > %d", deleteWrite.Seqnum, secondWrite.Seqnum)
	}

	value, err = handle.db.Get([]byte("beta"))
	if err != nil {
		t.Fatalf("Get(beta) after delete: %v", err)
	}
	if value != nil {
		t.Fatalf("Get(beta) after delete: got %q, want nil", *value)
	}
}

func TestDbScanVariants(t *testing.T) {
	store := newMemoryStore(t)
	handle := openTestDB(t, store, nil)

	seed := []struct {
		key   string
		value string
	}{
		{key: "item:01", value: "first"},
		{key: "item:02", value: "second"},
		{key: "item:03", value: "third"},
		{key: "other:01", value: "other"},
	}

	for _, entry := range seed {
		if _, err := handle.db.Put([]byte(entry.key), []byte(entry.value)); err != nil {
			t.Fatalf("Put(%q): %v", entry.key, err)
		}
	}

	iter, err := handle.db.Scan(slatedb.KeyRange{})
	if err != nil {
		t.Fatalf("Scan(full): %v", err)
	}
	t.Cleanup(iter.Destroy)
	requireRows(t, drainIterator(t, iter), []string{"item:01", "item:02", "item:03", "other:01"}, []string{"first", "second", "third", "other"})

	rangeStart := bytesPtr([]byte("item:02"))
	rangeEnd := bytesPtr([]byte("item:03"))
	iter, err = handle.db.Scan(slatedb.KeyRange{
		Start:          rangeStart,
		StartInclusive: true,
		End:            rangeEnd,
		EndInclusive:   true,
	})
	if err != nil {
		t.Fatalf("Scan(bounded): %v", err)
	}
	t.Cleanup(iter.Destroy)
	requireRows(t, drainIterator(t, iter), []string{"item:02", "item:03"}, []string{"second", "third"})

	iter, err = handle.db.ScanWithOptions(
		slatedb.KeyRange{
			Start:          bytesPtr([]byte("item:01")),
			StartInclusive: true,
			End:            bytesPtr([]byte("item:99")),
			EndInclusive:   false,
		},
		slatedb.ScanOptions{
			DurabilityFilter: slatedb.DurabilityLevelMemory,
			Dirty:            false,
			ReadAheadBytes:   64,
			CacheBlocks:      true,
			MaxFetchTasks:    2,
		},
	)
	if err != nil {
		t.Fatalf("ScanWithOptions(): %v", err)
	}
	t.Cleanup(iter.Destroy)
	requireRows(t, drainIterator(t, iter), []string{"item:01", "item:02", "item:03"}, []string{"first", "second", "third"})

	iter, err = handle.db.ScanPrefix([]byte("item:"))
	if err != nil {
		t.Fatalf("ScanPrefix(): %v", err)
	}
	t.Cleanup(iter.Destroy)
	requireRows(t, drainIterator(t, iter), []string{"item:01", "item:02", "item:03"}, []string{"first", "second", "third"})

	iter, err = handle.db.ScanPrefixWithOptions(
		[]byte("item:"),
		slatedb.ScanOptions{
			DurabilityFilter: slatedb.DurabilityLevelMemory,
			Dirty:            false,
			ReadAheadBytes:   32,
			CacheBlocks:      false,
			MaxFetchTasks:    1,
		},
	)
	if err != nil {
		t.Fatalf("ScanPrefixWithOptions(): %v", err)
	}
	t.Cleanup(iter.Destroy)
	requireRows(t, drainIterator(t, iter), []string{"item:01", "item:02", "item:03"}, []string{"first", "second", "third"})
}

func TestDbBatchWriteAndConsumption(t *testing.T) {
	store := newMemoryStore(t)
	handle := openTestDB(t, store, nil)

	if _, err := handle.db.Put([]byte("remove-me"), []byte("old")); err != nil {
		t.Fatalf("Put(remove-me): %v", err)
	}

	batch := slatedb.NewWriteBatch()
	t.Cleanup(batch.Destroy)

	if err := batch.Put([]byte("batch-put"), []byte("value")); err != nil {
		t.Fatalf("WriteBatch.Put(): %v", err)
	}
	if err := batch.Delete([]byte("remove-me")); err != nil {
		t.Fatalf("WriteBatch.Delete(): %v", err)
	}

	batchWrite, err := handle.db.Write(batch)
	if err != nil {
		t.Fatalf("Write(): %v", err)
	}
	if batchWrite.Seqnum == 0 {
		t.Fatalf("Write(): Seqnum = 0")
	}

	value, err := handle.db.Get([]byte("batch-put"))
	if err != nil {
		t.Fatalf("Get(batch-put): %v", err)
	}
	if value == nil || !bytes.Equal(*value, []byte("value")) {
		t.Fatalf("Get(batch-put): got %v, want %q", value, "value")
	}

	value, err = handle.db.Get([]byte("remove-me"))
	if err != nil {
		t.Fatalf("Get(remove-me): %v", err)
	}
	if value != nil {
		t.Fatalf("Get(remove-me): got %q, want nil", *value)
	}

	if _, err := handle.db.Write(batch); !errors.Is(err, slatedb.ErrErrorInvalid) {
		t.Fatalf("Write(consumed batch): got %v, want invalid error", err)
	}

	secondBatch := slatedb.NewWriteBatch()
	t.Cleanup(secondBatch.Destroy)

	if err := secondBatch.PutWithOptions([]byte("batch-put-2"), []byte("value-2"), slatedb.PutOptions{Ttl: slatedb.TtlDefault{}}); err != nil {
		t.Fatalf("WriteBatch.PutWithOptions(): %v", err)
	}

	if _, err := handle.db.WriteWithOptions(secondBatch, slatedb.WriteOptions{AwaitDurable: true}); err != nil {
		t.Fatalf("WriteWithOptions(): %v", err)
	}

	value, err = handle.db.Get([]byte("batch-put-2"))
	if err != nil {
		t.Fatalf("Get(batch-put-2): %v", err)
	}
	if value == nil || !bytes.Equal(*value, []byte("value-2")) {
		t.Fatalf("Get(batch-put-2): got %v, want %q", value, "value-2")
	}
}

func TestDbFlush(t *testing.T) {
	store := newMemoryStore(t)
	handle := openTestDB(t, store, nil)

	if _, err := handle.db.Put([]byte("flush-key"), []byte("value")); err != nil {
		t.Fatalf("Put(flush-key): %v", err)
	}

	if err := handle.db.Flush(); err != nil {
		t.Fatalf("Flush(): %v", err)
	}

	if err := handle.db.FlushWithOptions(slatedb.FlushOptions{FlushType: slatedb.FlushTypeWal}); err != nil {
		t.Fatalf("FlushWithOptions(Wal): %v", err)
	}

	if err := handle.db.FlushWithOptions(slatedb.FlushOptions{FlushType: slatedb.FlushTypeMemTable}); err != nil {
		t.Fatalf("FlushWithOptions(MemTable): %v", err)
	}

}

func TestDbMerge(t *testing.T) {
	store := newMemoryStore(t)
	handle := openTestDB(t, store, func(t *testing.T, builder *slatedb.DbBuilder) {
		t.Helper()
		if err := builder.WithMergeOperator(concatMergeOperator{}); err != nil {
			t.Fatalf("WithMergeOperator(): %v", err)
		}
	})

	if _, err := handle.db.Put([]byte("merge"), []byte("base")); err != nil {
		t.Fatalf("Put(merge): %v", err)
	}

	if _, err := handle.db.Merge([]byte("merge"), []byte(":one")); err != nil {
		t.Fatalf("Merge(): %v", err)
	}

	value, err := handle.db.Get([]byte("merge"))
	if err != nil {
		t.Fatalf("Get(merge) after Merge(): %v", err)
	}
	if value == nil || !bytes.Equal(*value, []byte("base:one")) {
		t.Fatalf("Get(merge) after Merge(): got %v, want %q", value, "base:one")
	}

	if _, err := handle.db.MergeWithOptions(
		[]byte("merge"),
		[]byte(":two"),
		slatedb.MergeOptions{Ttl: slatedb.TtlDefault{}},
		slatedb.WriteOptions{AwaitDurable: true},
	); err != nil {
		t.Fatalf("MergeWithOptions(): %v", err)
	}

	value, err = handle.db.Get([]byte("merge"))
	if err != nil {
		t.Fatalf("Get(merge) after MergeWithOptions(): %v", err)
	}
	if value == nil || !bytes.Equal(*value, []byte("base:one:two")) {
		t.Fatalf("Get(merge) after MergeWithOptions(): got %v, want %q", value, "base:one:two")
	}
}

func TestDbSnapshot(t *testing.T) {
	store := newMemoryStore(t)
	handle := openTestDB(t, store, nil)

	if _, err := handle.db.Put([]byte("snapshot"), []byte("old")); err != nil {
		t.Fatalf("Put(snapshot old): %v", err)
	}

	snapshot, err := handle.db.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot(): %v", err)
	}
	t.Cleanup(snapshot.Destroy)

	if _, err := handle.db.Put([]byte("snapshot"), []byte("new")); err != nil {
		t.Fatalf("Put(snapshot new): %v", err)
	}

	snapshotValue, err := snapshot.Get([]byte("snapshot"))
	if err != nil {
		t.Fatalf("Snapshot.Get(): %v", err)
	}
	if snapshotValue == nil || !bytes.Equal(*snapshotValue, []byte("old")) {
		t.Fatalf("Snapshot.Get(): got %v, want %q", snapshotValue, "old")
	}

	liveValue, err := handle.db.Get([]byte("snapshot"))
	if err != nil {
		t.Fatalf("Db.Get(snapshot): %v", err)
	}
	if liveValue == nil || !bytes.Equal(*liveValue, []byte("new")) {
		t.Fatalf("Db.Get(snapshot): got %v, want %q", liveValue, "new")
	}
}

func TestDbTransactions(t *testing.T) {
	store := newMemoryStore(t)
	handle := openTestDB(t, store, nil)

	tx, err := handle.db.Begin(slatedb.IsolationLevelSnapshot)
	if err != nil {
		t.Fatalf("Begin(): %v", err)
	}
	t.Cleanup(tx.Destroy)

	if tx.Id() == "" {
		t.Fatal("Begin(): transaction id is empty")
	}

	if err := tx.Put([]byte("txn-key"), []byte("pending")); err != nil {
		t.Fatalf("tx.Put(): %v", err)
	}

	txValue, err := tx.Get([]byte("txn-key"))
	if err != nil {
		t.Fatalf("tx.Get(): %v", err)
	}
	if txValue == nil || !bytes.Equal(*txValue, []byte("pending")) {
		t.Fatalf("tx.Get(): got %v, want %q", txValue, "pending")
	}

	liveValue, err := handle.db.Get([]byte("txn-key"))
	if err != nil {
		t.Fatalf("db.Get(txn-key) before commit: %v", err)
	}
	if liveValue != nil {
		t.Fatalf("db.Get(txn-key) before commit: got %q, want nil", *liveValue)
	}

	commitHandle, err := tx.Commit()
	if err != nil {
		t.Fatalf("tx.Commit(): %v", err)
	}
	if commitHandle == nil || commitHandle.Seqnum == 0 {
		t.Fatalf("tx.Commit(): got %v, want non-nil write handle", commitHandle)
	}

	liveValue, err = handle.db.Get([]byte("txn-key"))
	if err != nil {
		t.Fatalf("db.Get(txn-key) after commit: %v", err)
	}
	if liveValue == nil || !bytes.Equal(*liveValue, []byte("pending")) {
		t.Fatalf("db.Get(txn-key) after commit: got %v, want %q", liveValue, "pending")
	}

	rollbackTx, err := handle.db.Begin(slatedb.IsolationLevelSnapshot)
	if err != nil {
		t.Fatalf("Begin() for rollback: %v", err)
	}
	t.Cleanup(rollbackTx.Destroy)

	if err := rollbackTx.Put([]byte("rolled-back"), []byte("value")); err != nil {
		t.Fatalf("rollbackTx.Put(): %v", err)
	}
	if err := rollbackTx.Rollback(); err != nil {
		t.Fatalf("rollbackTx.Rollback(): %v", err)
	}

	liveValue, err = handle.db.Get([]byte("rolled-back"))
	if err != nil {
		t.Fatalf("db.Get(rolled-back): %v", err)
	}
	if liveValue != nil {
		t.Fatalf("db.Get(rolled-back): got %q, want nil", *liveValue)
	}
}

func TestDbInvalidInputsAndErrorMapping(t *testing.T) {
	t.Run("invalid inputs", func(t *testing.T) {
		store := newMemoryStore(t)
		handle := openTestDB(t, store, nil)

		if _, err := handle.db.Put([]byte{}, []byte("value")); !errors.Is(err, slatedb.ErrErrorInvalid) {
			t.Fatalf("Put(empty key): got %v, want invalid error", err)
		}

		if _, err := handle.db.Delete([]byte{}); !errors.Is(err, slatedb.ErrErrorInvalid) {
			t.Fatalf("Delete(empty key): got %v, want invalid error", err)
		}

		if _, err := handle.db.Scan(slatedb.KeyRange{
			Start:          bytesPtr([]byte("z")),
			StartInclusive: true,
			End:            bytesPtr([]byte("a")),
			EndInclusive:   true,
		}); !errors.Is(err, slatedb.ErrErrorInvalid) {
			t.Fatalf("Scan(start > end): got %v, want invalid error", err)
		}

		// Scan with empty start bound should succeed and be treated as unbounded start.
		if _, err := handle.db.Put([]byte("seed"), []byte("value")); err != nil {
			t.Fatalf("Put(seed): %v", err)
		}
		iter, err := handle.db.Scan(slatedb.KeyRange{
			Start:          bytesPtr([]byte{}),
			StartInclusive: true,
		})
		if err != nil {
			t.Fatalf("Scan(empty start): %v", err)
		}
		t.Cleanup(iter.Destroy)
		requireRows(t, drainIterator(t, iter), []string{"seed"}, []string{"value"})

		batch := slatedb.NewWriteBatch()
		t.Cleanup(batch.Destroy)
		if err := batch.Put([]byte("batch"), []byte("value")); err != nil {
			t.Fatalf("WriteBatch.Put(): %v", err)
		}
		if _, err := handle.db.Write(batch); err != nil {
			t.Fatalf("Write(): %v", err)
		}
		if _, err := handle.db.Write(batch); !errors.Is(err, slatedb.ErrErrorInvalid) {
			t.Fatalf("Write(consumed batch): got %v, want invalid error", err)
		}
	})

	t.Run("writer fencing", func(t *testing.T) {
		store := newMemoryStore(t)
		primary := openTestDB(t, store, nil)

		if _, err := primary.db.Put([]byte("primary"), []byte("value")); err != nil {
			t.Fatalf("primary Put(): %v", err)
		}

		secondary := openTestDB(t, store, nil)
		if _, err := secondary.db.Put([]byte("secondary"), []byte("value")); err != nil {
			t.Fatalf("secondary Put(): %v", err)
		}

		_, err := primary.db.Put([]byte("stale"), []byte("value"))
		if !errors.Is(err, slatedb.ErrErrorClosed) {
			t.Fatalf("primary Put() after fencing: got %v, want closed error", err)
		}
		primary.open = false

		var closedErr *slatedb.ErrorClosed
		if !errors.As(err, &closedErr) {
			t.Fatalf("primary Put() after fencing: expected *ErrorClosed, got %T", err)
		}
		if closedErr.Reason != slatedb.CloseReasonFenced {
			t.Fatalf("primary Put() after fencing: got close reason %v, want %v", closedErr.Reason, slatedb.CloseReasonFenced)
		}
	})
}

func TestDbReaderLifecycleAndMissingDb(t *testing.T) {
	t.Run("lifecycle", func(t *testing.T) {
		store := newMemoryStore(t)
		dbHandle := openTestDB(t, store, nil)

		if _, err := dbHandle.db.Put([]byte("reader"), []byte("value")); err != nil {
			t.Fatalf("Put(reader): %v", err)
		}
		if err := dbHandle.db.FlushWithOptions(slatedb.FlushOptions{FlushType: slatedb.FlushTypeMemTable}); err != nil {
			t.Fatalf("FlushWithOptions(MemTable): %v", err)
		}

		readerHandle := openTestReader(t, store, nil)

		value, err := readerHandle.reader.Get([]byte("reader"))
		if err != nil {
			t.Fatalf("DbReader.Get(): %v", err)
		}
		if value == nil || !bytes.Equal(*value, []byte("value")) {
			t.Fatalf("DbReader.Get(): got %v, want %q", value, "value")
		}

		if err := readerHandle.reader.Shutdown(); err != nil {
			t.Fatalf("DbReader.Shutdown(): %v", err)
		}
		readerHandle.open = false

		if _, err := readerHandle.reader.Get([]byte("reader")); !errors.Is(err, slatedb.ErrErrorClosed) {
			t.Fatalf("DbReader.Get() after Shutdown(): got %v, want closed error", err)
		}
	})

	t.Run("missing db", func(t *testing.T) {
		store := newMemoryStore(t)
		builder := slatedb.NewDbReaderBuilder(testDBPath, store)
		defer builder.Destroy()

		_, err := builder.Build()
		if !errors.Is(err, slatedb.ErrErrorData) {
			t.Fatalf("DbReaderBuilder.Build() without db: got %v, want data error", err)
		}
	})
}

func TestDbReaderPointReads(t *testing.T) {
	store := newMemoryStore(t)
	dbHandle := openTestDB(t, store, nil)

	if _, err := dbHandle.db.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("Put(alpha): %v", err)
	}
	if _, err := dbHandle.db.Put([]byte("empty"), []byte{}); err != nil {
		t.Fatalf("Put(empty): %v", err)
	}
	if err := dbHandle.db.FlushWithOptions(slatedb.FlushOptions{FlushType: slatedb.FlushTypeMemTable}); err != nil {
		t.Fatalf("FlushWithOptions(MemTable): %v", err)
	}

	readerHandle := openTestReader(t, store, nil)

	value, err := readerHandle.reader.Get([]byte("alpha"))
	if err != nil {
		t.Fatalf("DbReader.Get(alpha): %v", err)
	}
	if value == nil || !bytes.Equal(*value, []byte("one")) {
		t.Fatalf("DbReader.Get(alpha): got %v, want %q", value, "one")
	}

	value, err = readerHandle.reader.GetWithOptions([]byte("alpha"), slatedb.ReadOptions{
		DurabilityFilter: slatedb.DurabilityLevelMemory,
		Dirty:            false,
		CacheBlocks:      true,
	})
	if err != nil {
		t.Fatalf("DbReader.GetWithOptions(alpha): %v", err)
	}
	if value == nil || !bytes.Equal(*value, []byte("one")) {
		t.Fatalf("DbReader.GetWithOptions(alpha): got %v, want %q", value, "one")
	}

	value, err = readerHandle.reader.Get([]byte("empty"))
	if err != nil {
		t.Fatalf("DbReader.Get(empty): %v", err)
	}
	if value == nil || len(*value) != 0 {
		t.Fatalf("DbReader.Get(empty): got %v, want empty slice", value)
	}

	value, err = readerHandle.reader.Get([]byte("missing"))
	if err != nil {
		t.Fatalf("DbReader.Get(missing): %v", err)
	}
	if value != nil {
		t.Fatalf("DbReader.Get(missing): got %q, want nil", *value)
	}
}

func TestDbReaderScanVariants(t *testing.T) {
	store := newMemoryStore(t)
	dbHandle := openTestDB(t, store, nil)

	seed := []struct {
		key   string
		value string
	}{
		{key: "item:01", value: "first"},
		{key: "item:02", value: "second"},
		{key: "item:03", value: "third"},
		{key: "other:01", value: "other"},
	}

	for _, entry := range seed {
		if _, err := dbHandle.db.Put([]byte(entry.key), []byte(entry.value)); err != nil {
			t.Fatalf("Put(%q): %v", entry.key, err)
		}
	}
	if err := dbHandle.db.FlushWithOptions(slatedb.FlushOptions{FlushType: slatedb.FlushTypeMemTable}); err != nil {
		t.Fatalf("FlushWithOptions(MemTable): %v", err)
	}

	readerHandle := openTestReader(t, store, nil)

	iter, err := readerHandle.reader.Scan(slatedb.KeyRange{})
	if err != nil {
		t.Fatalf("DbReader.Scan(full): %v", err)
	}
	t.Cleanup(iter.Destroy)
	requireRows(t, drainIterator(t, iter), []string{"item:01", "item:02", "item:03", "other:01"}, []string{"first", "second", "third", "other"})

	iter, err = readerHandle.reader.Scan(slatedb.KeyRange{
		Start:          bytesPtr([]byte("item:02")),
		StartInclusive: true,
		End:            bytesPtr([]byte("item:03")),
		EndInclusive:   true,
	})
	if err != nil {
		t.Fatalf("DbReader.Scan(bounded): %v", err)
	}
	t.Cleanup(iter.Destroy)
	requireRows(t, drainIterator(t, iter), []string{"item:02", "item:03"}, []string{"second", "third"})

	iter, err = readerHandle.reader.ScanWithOptions(
		slatedb.KeyRange{
			Start:          bytesPtr([]byte("item:01")),
			StartInclusive: true,
			End:            bytesPtr([]byte("item:99")),
			EndInclusive:   false,
		},
		slatedb.ScanOptions{
			DurabilityFilter: slatedb.DurabilityLevelMemory,
			Dirty:            false,
			ReadAheadBytes:   64,
			CacheBlocks:      true,
			MaxFetchTasks:    2,
		},
	)
	if err != nil {
		t.Fatalf("DbReader.ScanWithOptions(): %v", err)
	}
	t.Cleanup(iter.Destroy)
	requireRows(t, drainIterator(t, iter), []string{"item:01", "item:02", "item:03"}, []string{"first", "second", "third"})

	iter, err = readerHandle.reader.ScanPrefix([]byte("item:"))
	if err != nil {
		t.Fatalf("DbReader.ScanPrefix(): %v", err)
	}
	t.Cleanup(iter.Destroy)
	requireRows(t, drainIterator(t, iter), []string{"item:01", "item:02", "item:03"}, []string{"first", "second", "third"})

	iter, err = readerHandle.reader.ScanPrefixWithOptions(
		[]byte("item:"),
		slatedb.ScanOptions{
			DurabilityFilter: slatedb.DurabilityLevelMemory,
			Dirty:            false,
			ReadAheadBytes:   32,
			CacheBlocks:      false,
			MaxFetchTasks:    1,
		},
	)
	if err != nil {
		t.Fatalf("DbReader.ScanPrefixWithOptions(): %v", err)
	}
	t.Cleanup(iter.Destroy)
	requireRows(t, drainIterator(t, iter), []string{"item:01", "item:02", "item:03"}, []string{"first", "second", "third"})
}

func TestDbReaderRefreshBehavior(t *testing.T) {
	store := newMemoryStore(t)
	dbHandle := openTestDB(t, store, nil)

	if _, err := dbHandle.db.Put([]byte("seed"), []byte("value")); err != nil {
		t.Fatalf("Put(seed): %v", err)
	}
	if err := dbHandle.db.FlushWithOptions(slatedb.FlushOptions{FlushType: slatedb.FlushTypeMemTable}); err != nil {
		t.Fatalf("FlushWithOptions(MemTable): %v", err)
	}

	readerHandle := openTestReader(t, store, func(t *testing.T, builder *slatedb.DbReaderBuilder) {
		t.Helper()
		if err := builder.WithOptions(slatedb.ReaderOptions{
			ManifestPollIntervalMs: 100,
			CheckpointLifetimeMs:   1000,
			MaxMemtableBytes:       64 * 1024 * 1024,
			SkipWalReplay:          false,
		}); err != nil {
			t.Fatalf("DbReaderBuilder.WithOptions(): %v", err)
		}
	})

	if _, err := dbHandle.db.Put([]byte("refresh"), []byte("updated")); err != nil {
		t.Fatalf("Put(refresh): %v", err)
	}
	if err := dbHandle.db.FlushWithOptions(slatedb.FlushOptions{FlushType: slatedb.FlushTypeMemTable}); err != nil {
		t.Fatalf("FlushWithOptions(MemTable): %v", err)
	}

	waitUntil(t, 60*time.Second, 25*time.Millisecond, func() (bool, error) {
		value, err := readerHandle.reader.Get([]byte("refresh"))
		if err != nil {
			return false, err
		}
		return value != nil && bytes.Equal(*value, []byte("updated")), nil
	})
}

func TestDbReaderWalReplayBehavior(t *testing.T) {
	t.Run("default reader replays new wal data", func(t *testing.T) {
		store := newMemoryStore(t)
		dbHandle := openTestDB(t, store, nil)
		readerHandle := openTestReader(t, store, func(t *testing.T, builder *slatedb.DbReaderBuilder) {
			t.Helper()
			if err := builder.WithOptions(slatedb.ReaderOptions{
				ManifestPollIntervalMs: 100,
				CheckpointLifetimeMs:   1000,
				MaxMemtableBytes:       64 * 1024 * 1024,
				SkipWalReplay:          false,
			}); err != nil {
				t.Fatalf("DbReaderBuilder.WithOptions(): %v", err)
			}
		})

		if _, err := dbHandle.db.Put([]byte("wal-key"), []byte("wal-value")); err != nil {
			t.Fatalf("Put(wal-key): %v", err)
		}
		if err := dbHandle.db.FlushWithOptions(slatedb.FlushOptions{FlushType: slatedb.FlushTypeWal}); err != nil {
			t.Fatalf("FlushWithOptions(Wal): %v", err)
		}

		waitUntil(t, 60*time.Second, 25*time.Millisecond, func() (bool, error) {
			value, err := readerHandle.reader.Get([]byte("wal-key"))
			if err != nil {
				return false, err
			}
			return value != nil && bytes.Equal(*value, []byte("wal-value")), nil
		})
	})

	t.Run("skip wal replay ignores wal-only data", func(t *testing.T) {
		store := newMemoryStore(t)
		dbHandle := openTestDB(t, store, nil)

		if _, err := dbHandle.db.Put([]byte("flushed-key"), []byte("flushed-value")); err != nil {
			t.Fatalf("Put(flushed-key): %v", err)
		}
		if err := dbHandle.db.FlushWithOptions(slatedb.FlushOptions{FlushType: slatedb.FlushTypeMemTable}); err != nil {
			t.Fatalf("FlushWithOptions(MemTable) for flushed-key: %v", err)
		}

		if _, err := dbHandle.db.Put([]byte("wal-only-key"), []byte("wal-only-value")); err != nil {
			t.Fatalf("Put(wal-only-key): %v", err)
		}
		if err := dbHandle.db.FlushWithOptions(slatedb.FlushOptions{FlushType: slatedb.FlushTypeWal}); err != nil {
			t.Fatalf("FlushWithOptions(Wal) for wal-only-key: %v", err)
		}

		readerHandle := openTestReader(t, store, func(t *testing.T, builder *slatedb.DbReaderBuilder) {
			t.Helper()
			if err := builder.WithOptions(slatedb.ReaderOptions{
				ManifestPollIntervalMs: 100,
				CheckpointLifetimeMs:   1000,
				MaxMemtableBytes:       64 * 1024 * 1024,
				SkipWalReplay:          true,
			}); err != nil {
				t.Fatalf("DbReaderBuilder.WithOptions(): %v", err)
			}
		})

		value, err := readerHandle.reader.Get([]byte("flushed-key"))
		if err != nil {
			t.Fatalf("DbReader.Get(flushed-key): %v", err)
		}
		if value == nil || !bytes.Equal(*value, []byte("flushed-value")) {
			t.Fatalf("DbReader.Get(flushed-key): got %v, want %q", value, "flushed-value")
		}

		value, err = readerHandle.reader.Get([]byte("wal-only-key"))
		if err != nil {
			t.Fatalf("DbReader.Get(wal-only-key): %v", err)
		}
		if value != nil {
			t.Fatalf("DbReader.Get(wal-only-key): got %q, want nil", *value)
		}

		if err := dbHandle.db.FlushWithOptions(slatedb.FlushOptions{FlushType: slatedb.FlushTypeMemTable}); err != nil {
			t.Fatalf("FlushWithOptions(MemTable) for wal-only-key: %v", err)
		}

		readerHandle2 := openTestReader(t, store, func(t *testing.T, builder *slatedb.DbReaderBuilder) {
			t.Helper()
			if err := builder.WithOptions(slatedb.ReaderOptions{
				ManifestPollIntervalMs: 100,
				CheckpointLifetimeMs:   1000,
				MaxMemtableBytes:       64 * 1024 * 1024,
				SkipWalReplay:          true,
			}); err != nil {
				t.Fatalf("DbReaderBuilder.WithOptions(): %v", err)
			}
		})

		value, err = readerHandle2.reader.Get([]byte("wal-only-key"))
		if err != nil {
			t.Fatalf("DbReader.Get(wal-only-key) after memtable flush: %v", err)
		}
		if value == nil || !bytes.Equal(*value, []byte("wal-only-value")) {
			t.Fatalf("DbReader.Get(wal-only-key) after memtable flush: got %v, want %q", value, "wal-only-value")
		}
	})
}

func TestDbReaderMergeOperator(t *testing.T) {
	store := newMemoryStore(t)
	dbHandle := openTestDB(t, store, func(t *testing.T, builder *slatedb.DbBuilder) {
		t.Helper()
		if err := builder.WithMergeOperator(concatMergeOperator{}); err != nil {
			t.Fatalf("DbBuilder.WithMergeOperator(): %v", err)
		}
	})

	if _, err := dbHandle.db.Put([]byte("merge"), []byte("base")); err != nil {
		t.Fatalf("Put(merge): %v", err)
	}
	if _, err := dbHandle.db.Merge([]byte("merge"), []byte(":reader")); err != nil {
		t.Fatalf("Merge(merge): %v", err)
	}
	if err := dbHandle.db.FlushWithOptions(slatedb.FlushOptions{FlushType: slatedb.FlushTypeMemTable}); err != nil {
		t.Fatalf("FlushWithOptions(MemTable): %v", err)
	}

	readerHandle := openTestReader(t, store, func(t *testing.T, builder *slatedb.DbReaderBuilder) {
		t.Helper()
		if err := builder.WithMergeOperator(concatMergeOperator{}); err != nil {
			t.Fatalf("DbReaderBuilder.WithMergeOperator(): %v", err)
		}
	})

	value, err := readerHandle.reader.Get([]byte("merge"))
	if err != nil {
		t.Fatalf("DbReader.Get(merge): %v", err)
	}
	if value == nil || !bytes.Equal(*value, []byte("base:reader")) {
		t.Fatalf("DbReader.Get(merge): got %v, want %q", value, "base:reader")
	}
}

func TestDbReaderBuilderValidationAndErrors(t *testing.T) {
	t.Run("invalid checkpoint id", func(t *testing.T) {
		store := newMemoryStore(t)
		dbHandle := openTestDB(t, store, nil)
		if _, err := dbHandle.db.Put([]byte("seed"), []byte("value")); err != nil {
			t.Fatalf("Put(seed): %v", err)
		}
		if err := dbHandle.db.FlushWithOptions(slatedb.FlushOptions{FlushType: slatedb.FlushTypeMemTable}); err != nil {
			t.Fatalf("FlushWithOptions(MemTable): %v", err)
		}

		builder := slatedb.NewDbReaderBuilder(testDBPath, store)
		defer builder.Destroy()

		err := builder.WithCheckpointId("not-a-uuid")
		if !errors.Is(err, slatedb.ErrErrorInvalid) {
			t.Fatalf("DbReaderBuilder.WithCheckpointId(invalid): got %v, want invalid error", err)
		}
	})

	t.Run("missing checkpoint id", func(t *testing.T) {
		store := newMemoryStore(t)
		dbHandle := openTestDB(t, store, nil)
		if _, err := dbHandle.db.Put([]byte("seed"), []byte("value")); err != nil {
			t.Fatalf("Put(seed): %v", err)
		}
		if err := dbHandle.db.FlushWithOptions(slatedb.FlushOptions{FlushType: slatedb.FlushTypeMemTable}); err != nil {
			t.Fatalf("FlushWithOptions(MemTable): %v", err)
		}

		builder := slatedb.NewDbReaderBuilder(testDBPath, store)
		defer builder.Destroy()

		if err := builder.WithCheckpointId("ffffffff-ffff-ffff-ffff-ffffffffffff"); err != nil {
			t.Fatalf("DbReaderBuilder.WithCheckpointId(valid): %v", err)
		}

		_, err := builder.Build()
		if !errors.Is(err, slatedb.ErrErrorData) {
			t.Fatalf("DbReaderBuilder.Build() with missing checkpoint: got %v, want data error", err)
		}
	})

	t.Run("builder consumed after build", func(t *testing.T) {
		store := newMemoryStore(t)
		dbHandle := openTestDB(t, store, nil)
		if _, err := dbHandle.db.Put([]byte("seed"), []byte("value")); err != nil {
			t.Fatalf("Put(seed): %v", err)
		}
		if err := dbHandle.db.FlushWithOptions(slatedb.FlushOptions{FlushType: slatedb.FlushTypeMemTable}); err != nil {
			t.Fatalf("FlushWithOptions(MemTable): %v", err)
		}

		builder := slatedb.NewDbReaderBuilder(testDBPath, store)
		defer builder.Destroy()

		reader, err := builder.Build()
		if err != nil {
			t.Fatalf("DbReaderBuilder.Build(): %v", err)
		}
		defer reader.Destroy()
		if err := reader.Shutdown(); err != nil {
			t.Fatalf("DbReader.Shutdown(): %v", err)
		}

		_, err = builder.Build()
		if !errors.Is(err, slatedb.ErrErrorInvalid) {
			t.Fatalf("second DbReaderBuilder.Build(): got %v, want invalid error", err)
		}
	})

	t.Run("invalid key range", func(t *testing.T) {
		store := newMemoryStore(t)
		dbHandle := openTestDB(t, store, nil)
		if _, err := dbHandle.db.Put([]byte("seed"), []byte("value")); err != nil {
			t.Fatalf("Put(seed): %v", err)
		}
		if err := dbHandle.db.FlushWithOptions(slatedb.FlushOptions{FlushType: slatedb.FlushTypeMemTable}); err != nil {
			t.Fatalf("FlushWithOptions(MemTable): %v", err)
		}

		readerHandle := openTestReader(t, store, nil)

		if _, err := readerHandle.reader.Scan(slatedb.KeyRange{
			Start:          bytesPtr([]byte("z")),
			StartInclusive: true,
			End:            bytesPtr([]byte("a")),
			EndInclusive:   true,
		}); !errors.Is(err, slatedb.ErrErrorInvalid) {
			t.Fatalf("DbReader.Scan(start > end): got %v, want invalid error", err)
		}

		// Scan with empty start bound should succeed and be treated as unbounded start.
		readerIter, err := readerHandle.reader.Scan(slatedb.KeyRange{
			Start:          bytesPtr([]byte{}),
			StartInclusive: true,
		})
		if err != nil {
			t.Fatalf("DbReader.Scan(empty start): %v", err)
		}
		t.Cleanup(readerIter.Destroy)
		requireRows(t, drainIterator(t, readerIter), []string{"seed"}, []string{"value"})
	})
}

func TestAdminBuilderValidationAndErrors(t *testing.T) {
	t.Run("builder consumed after build", func(t *testing.T) {
		store := newMemoryStore(t)
		walStore := newMemoryStore(t)

		builder := slatedb.NewAdminBuilder(testDBPath, store)
		defer builder.Destroy()

		if err := builder.WithSeed(42); err != nil {
			t.Fatalf("AdminBuilder.WithSeed(): %v", err)
		}
		if err := builder.WithWalObjectStore(walStore); err != nil {
			t.Fatalf("AdminBuilder.WithWalObjectStore(): %v", err)
		}

		admin, err := builder.Build()
		if err != nil {
			t.Fatalf("AdminBuilder.Build(): %v", err)
		}
		defer admin.Destroy()

		_, err = builder.Build()
		if !errors.Is(err, slatedb.ErrErrorInvalid) {
			t.Fatalf("second AdminBuilder.Build(): got %v, want invalid error", err)
		}

		var invalidErr *slatedb.ErrorInvalid
		if !errors.As(err, &invalidErr) {
			t.Fatalf("second AdminBuilder.Build(): expected *ErrorInvalid, got %T", err)
		}
		if invalidErr.Message != "builder has already been consumed" {
			t.Fatalf("second AdminBuilder.Build(): message = %q, want %q", invalidErr.Message, "builder has already been consumed")
		}
	})

	t.Run("invalid timestamp seconds", func(t *testing.T) {
		store := newMemoryStore(t)
		admin := openTestAdmin(t, store, nil)

		_, err := admin.GetSequenceForTimestamp(1<<62, false)
		if !errors.Is(err, slatedb.ErrErrorInvalid) {
			t.Fatalf("GetSequenceForTimestamp(invalid): got %v, want invalid error", err)
		}

		var invalidErr *slatedb.ErrorInvalid
		if !errors.As(err, &invalidErr) {
			t.Fatalf("GetSequenceForTimestamp(invalid): expected *ErrorInvalid, got %T", err)
		}
		if !strings.Contains(invalidErr.Message, "invalid timestamp seconds") {
			t.Fatalf("GetSequenceForTimestamp(invalid): message = %q, want substring %q", invalidErr.Message, "invalid timestamp seconds")
		}
	})

	t.Run("invalid compaction id", func(t *testing.T) {
		store := newMemoryStore(t)
		admin := openTestAdmin(t, store, nil)

		_, err := admin.ReadCompaction("not-a-ulid", nil)
		if !errors.Is(err, slatedb.ErrErrorInvalid) {
			t.Fatalf("ReadCompaction(invalid): got %v, want invalid error", err)
		}

		var invalidErr *slatedb.ErrorInvalid
		if !errors.As(err, &invalidErr) {
			t.Fatalf("ReadCompaction(invalid): expected *ErrorInvalid, got %T", err)
		}
		if !strings.Contains(invalidErr.Message, "invalid compaction_id ULID") {
			t.Fatalf("ReadCompaction(invalid): message = %q, want substring %q", invalidErr.Message, "invalid compaction_id ULID")
		}
	})

	t.Run("reversed range returns empty", func(t *testing.T) {
		store := newMemoryStore(t)
		admin := openTestAdmin(t, store, nil)

		manifests, err := admin.ListManifests(uint64Ptr(2), uint64Ptr(1))
		if err != nil {
			t.Fatalf("ListManifests(reversed range): %v", err)
		}
		if len(manifests) != 0 {
			t.Fatalf("ListManifests(reversed range): got %d manifests, want 0", len(manifests))
		}
	})
}

func TestAdminQueries(t *testing.T) {
	store := newMemoryStore(t)
	walStore := newMemoryStore(t)

	dbHandle := openTestDB(t, store, func(t *testing.T, builder *slatedb.DbBuilder) {
		t.Helper()
		if err := builder.WithSeed(7); err != nil {
			t.Fatalf("DbBuilder.WithSeed(): %v", err)
		}
		if err := builder.WithWalObjectStore(walStore); err != nil {
			t.Fatalf("DbBuilder.WithWalObjectStore(): %v", err)
		}
	})

	if _, err := dbHandle.db.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("Put(k1): %v", err)
	}
	if _, err := dbHandle.db.Put([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("Put(k2): %v", err)
	}
	if err := dbHandle.db.FlushWithOptions(slatedb.FlushOptions{FlushType: slatedb.FlushTypeMemTable}); err != nil {
		t.Fatalf("FlushWithOptions(MemTable #1): %v", err)
	}
	if _, err := dbHandle.db.Put([]byte("k3"), []byte("v3")); err != nil {
		t.Fatalf("Put(k3): %v", err)
	}
	if err := dbHandle.db.FlushWithOptions(slatedb.FlushOptions{FlushType: slatedb.FlushTypeMemTable}); err != nil {
		t.Fatalf("FlushWithOptions(MemTable #2): %v", err)
	}

	admin := openTestAdmin(t, store, func(t *testing.T, builder *slatedb.AdminBuilder) {
		t.Helper()
		if err := builder.WithSeed(7); err != nil {
			t.Fatalf("AdminBuilder.WithSeed(): %v", err)
		}
		if err := builder.WithWalObjectStore(walStore); err != nil {
			t.Fatalf("AdminBuilder.WithWalObjectStore(): %v", err)
		}
	})

	var manifests []slatedb.VersionedManifest
	waitUntil(t, 10*time.Second, 10*time.Millisecond, func() (bool, error) {
		got, err := admin.ListManifests(nil, nil)
		if err != nil {
			return false, err
		}
		manifests = got
		return len(manifests) >= 3, nil
	})

	for i := 1; i < len(manifests); i++ {
		if manifests[i-1].Id >= manifests[i].Id {
			t.Fatalf("ListManifests(): ids not strictly ascending at %d: %d then %d", i, manifests[i-1].Id, manifests[i].Id)
		}
	}

	latestManifest, err := admin.ReadManifest(nil)
	if err != nil {
		t.Fatalf("ReadManifest(nil): %v", err)
	}
	if latestManifest == nil {
		t.Fatal("ReadManifest(nil): got nil manifest")
	}
	if latestManifest.Id != manifests[len(manifests)-1].Id {
		t.Fatalf("ReadManifest(nil): got id %d, want latest id %d", latestManifest.Id, manifests[len(manifests)-1].Id)
	}
	if latestManifest.LastL0Seq < 3 {
		t.Fatalf("ReadManifest(nil): LastL0Seq = %d, want at least 3", latestManifest.LastL0Seq)
	}
	if latestManifest.WalObjectStoreUri == nil {
		t.Fatal("ReadManifest(nil): WalObjectStoreUri = nil, want value for configured WAL store")
	}

	firstManifest, err := admin.ReadManifest(uint64Ptr(manifests[0].Id))
	if err != nil {
		t.Fatalf("ReadManifest(first): %v", err)
	}
	if firstManifest == nil {
		t.Fatal("ReadManifest(first): got nil manifest")
	}
	if firstManifest.Id != manifests[0].Id {
		t.Fatalf("ReadManifest(first): got id %d, want %d", firstManifest.Id, manifests[0].Id)
	}

	boundedManifests, err := admin.ListManifests(uint64Ptr(manifests[1].Id), uint64Ptr(latestManifest.Id))
	if err != nil {
		t.Fatalf("ListManifests(bounded): %v", err)
	}
	if len(boundedManifests) == 0 {
		t.Fatal("ListManifests(bounded): got empty result")
	}
	for _, manifest := range boundedManifests {
		if manifest.Id < manifests[1].Id || manifest.Id >= latestManifest.Id {
			t.Fatalf("ListManifests(bounded): got id %d outside [%d, %d)", manifest.Id, manifests[1].Id, latestManifest.Id)
		}
	}

	checkpoints, err := admin.ListCheckpoints(nil)
	if err != nil {
		t.Fatalf("ListCheckpoints(nil): %v", err)
	}
	if len(checkpoints) != 0 {
		t.Fatalf("ListCheckpoints(nil): got %d checkpoints, want 0", len(checkpoints))
	}

	checkpointName := "named-checkpoint"
	filteredCheckpoints, err := admin.ListCheckpoints(&checkpointName)
	if err != nil {
		t.Fatalf("ListCheckpoints(name): %v", err)
	}
	if len(filteredCheckpoints) != 0 {
		t.Fatalf("ListCheckpoints(name): got %d checkpoints, want 0", len(filteredCheckpoints))
	}

	compactions, err := admin.ListCompactions(nil, nil)
	if err != nil {
		t.Fatalf("ListCompactions(): %v", err)
	}
	if len(compactions) == 0 {
		t.Fatal("ListCompactions(): got empty result")
	}
	for i := 1; i < len(compactions); i++ {
		if compactions[i-1].Id >= compactions[i].Id {
			t.Fatalf("ListCompactions(): ids not strictly ascending at %d: %d then %d", i, compactions[i-1].Id, compactions[i].Id)
		}
	}

	latestCompactions, err := admin.ReadCompactions(nil)
	if err != nil {
		t.Fatalf("ReadCompactions(nil): %v", err)
	}
	if latestCompactions == nil {
		t.Fatal("ReadCompactions(nil): got nil compactions")
	}
	if latestCompactions.Id != compactions[len(compactions)-1].Id {
		t.Fatalf("ReadCompactions(nil): got id %d, want latest id %d", latestCompactions.Id, compactions[len(compactions)-1].Id)
	}

	compaction, err := admin.ReadCompaction("01ARZ3NDEKTSV4RRFFQ69G5FAV", nil)
	if err != nil {
		t.Fatalf("ReadCompaction(missing): %v", err)
	}
	if compaction != nil {
		t.Fatalf("ReadCompaction(missing): got %+v, want nil", compaction)
	}

	stateView, err := admin.ReadCompactorStateView()
	if err != nil {
		t.Fatalf("ReadCompactorStateView(): %v", err)
	}
	if stateView.Compactions == nil {
		t.Fatal("ReadCompactorStateView(): got nil compactions")
	}
	if stateView.Compactions.Id != latestCompactions.Id {
		t.Fatalf("ReadCompactorStateView(): compactions id = %d, want %d", stateView.Compactions.Id, latestCompactions.Id)
	}
	if stateView.Manifest.Id != latestManifest.Id {
		t.Fatalf("ReadCompactorStateView(): manifest id = %d, want %d", stateView.Manifest.Id, latestManifest.Id)
	}

	firstTimestamp, err := admin.GetTimestampForSequence(0, true)
	if err != nil {
		t.Fatalf("GetTimestampForSequence(0, true): %v", err)
	}
	if firstTimestamp == nil {
		t.Fatal("GetTimestampForSequence(0, true): got nil timestamp")
	}

	afterLastTimestamp, err := admin.GetTimestampForSequence(^uint64(0), true)
	if err != nil {
		t.Fatalf("GetTimestampForSequence(after last, true): %v", err)
	}
	if afterLastTimestamp != nil {
		t.Fatalf("GetTimestampForSequence(after last, true): got %v, want nil", *afterLastTimestamp)
	}

	beforeFirstSequence, err := admin.GetSequenceForTimestamp(1, false)
	if err != nil {
		t.Fatalf("GetSequenceForTimestamp(before first, false): %v", err)
	}
	if beforeFirstSequence != nil {
		t.Fatalf("GetSequenceForTimestamp(before first, false): got %v, want nil", *beforeFirstSequence)
	}

	sequenceAtFirstTimestamp, err := admin.GetSequenceForTimestamp(*firstTimestamp, true)
	if err != nil {
		t.Fatalf("GetSequenceForTimestamp(first timestamp, true): %v", err)
	}
	if sequenceAtFirstTimestamp == nil {
		t.Fatal("GetSequenceForTimestamp(first timestamp, true): got nil sequence")
	}
	if *sequenceAtFirstTimestamp == 0 || *sequenceAtFirstTimestamp > latestManifest.LastL0Seq {
		t.Fatalf("GetSequenceForTimestamp(first timestamp, true): got %d, want range [1, %d]", *sequenceAtFirstTimestamp, latestManifest.LastL0Seq)
	}
}

func TestWalReaderEmptyStore(t *testing.T) {
	store := newMemoryStore(t)
	reader := openTestWalReader(t, store)

	files, err := reader.List(nil, nil)
	if err != nil {
		t.Fatalf("WalReader.List(nil, nil): %v", err)
	}
	for _, file := range files {
		defer file.Destroy()
	}

	if len(files) != 0 {
		t.Fatalf("WalReader.List(nil, nil): got %d files, want 0", len(files))
	}
}

func TestWalReaderListingAndNavigation(t *testing.T) {
	store := newMemoryStore(t)
	seedWalFiles(t, store)

	reader := openTestWalReader(t, store)

	files, err := reader.List(nil, nil)
	if err != nil {
		t.Fatalf("WalReader.List(nil, nil): %v", err)
	}

	if len(files) < 3 {
		t.Fatalf("WalReader.List(nil, nil): got %d files, want at least 3", len(files))
	}

	ids := make([]uint64, len(files))
	for i, file := range files {
		ids[i] = file.Id()
		if i > 0 && ids[i] <= ids[i-1] {
			t.Fatalf("WalReader.List(nil, nil): ids not ascending: %v", ids)
		}
	}

	startID := ids[1]
	endID := ids[2]
	bounded, err := reader.List(&startID, &endID)
	if err != nil {
		t.Fatalf("WalReader.List(start, end): %v", err)
	}
	for _, file := range bounded {
		defer file.Destroy()
	}

	if len(bounded) != 1 || bounded[0].Id() != ids[1] {
		t.Fatalf("WalReader.List(start, end): got ids [%d], want [%d]", len(bounded), ids[1])
	}

	pastHighID := ids[len(ids)-1] + 1000
	empty, err := reader.List(&pastHighID, nil)
	if err != nil {
		t.Fatalf("WalReader.List(pastHigh, nil): %v", err)
	}

	if len(empty) != 0 {
		t.Fatalf("WalReader.List(pastHigh, nil): got %d files, want 0", len(empty))
	}

	first := reader.Get(ids[0])
	defer first.Destroy()
	if first.Id() != ids[0] {
		t.Fatalf("WalReader.Get(first): got id %d, want %d", first.Id(), ids[0])
	}
	if first.NextId() != ids[1] {
		t.Fatalf("WalFile.NextId(): got %d, want %d", first.NextId(), ids[1])
	}

	next := first.NextFile()
	defer next.Destroy()
	if next.Id() != ids[1] {
		t.Fatalf("WalFile.NextFile().Id(): got %d, want %d", next.Id(), ids[1])
	}
}

func TestWalReaderMetadataAndRows(t *testing.T) {
	store := newMemoryStore(t)
	seedWalFiles(t, store)

	reader := openTestWalReader(t, store)

	files, err := reader.List(nil, nil)
	if err != nil {
		t.Fatalf("WalReader.List(nil, nil): %v", err)
	}
	for _, file := range files {
		defer file.Destroy()
	}

	if len(files) < 3 {
		t.Fatalf("WalReader.List(nil, nil): got %d files, want at least 3", len(files))
	}

	var allRows []slatedb.RowEntry

	for i, file := range files {
		metadata, err := file.Metadata()
		if err != nil {
			t.Fatalf("WalFile.Metadata() for file %d: %v", i, err)
		}
		if metadata.SizeBytes == 0 {
			t.Fatalf("WalFile.Metadata() for file %d: SizeBytes = 0", i)
		}
		if metadata.Location == "" {
			t.Fatalf("WalFile.Metadata() for file %d: Location is empty", i)
		}

		iter, err := file.Iterator()
		if err != nil {
			t.Fatalf("WalFile.Iterator() for file %d: %v", i, err)
		}
		t.Cleanup(iter.Destroy)

		rows := drainWalIterator(t, iter)
		for j, row := range rows {
			if row.Seq == 0 {
				t.Fatalf("row %d in file %d: Seq = 0", j, i)
			}
		}
		allRows = append(allRows, rows...)
	}

	if len(allRows) != 4 {
		t.Fatalf("unexpected total WAL row count: got %d, want 4", len(allRows))
	}

	if allRows[0].Kind != slatedb.RowEntryKindValue || string(allRows[0].Key) != "a" {
		t.Fatalf("row 0: got kind=%v key=%q, want value/a", allRows[0].Kind, allRows[0].Key)
	}
	if allRows[0].Value == nil || !bytes.Equal(*allRows[0].Value, []byte("1")) {
		t.Fatalf("row 0: got value %v, want %q", allRows[0].Value, "1")
	}

	if allRows[1].Kind != slatedb.RowEntryKindValue || string(allRows[1].Key) != "b" {
		t.Fatalf("row 1: got kind=%v key=%q, want value/b", allRows[1].Kind, allRows[1].Key)
	}
	if allRows[1].Value == nil || !bytes.Equal(*allRows[1].Value, []byte("2")) {
		t.Fatalf("row 1: got value %v, want %q", allRows[1].Value, "2")
	}

	if allRows[2].Kind != slatedb.RowEntryKindTombstone || string(allRows[2].Key) != "a" {
		t.Fatalf("row 2: got kind=%v key=%q, want tombstone/a", allRows[2].Kind, allRows[2].Key)
	}
	if allRows[2].Value != nil {
		t.Fatalf("row 2: got value %q, want nil", *allRows[2].Value)
	}

	if allRows[3].Kind != slatedb.RowEntryKindMerge || string(allRows[3].Key) != "m" {
		t.Fatalf("row 3: got kind=%v key=%q, want merge/m", allRows[3].Kind, allRows[3].Key)
	}
	if allRows[3].Value == nil || !bytes.Equal(*allRows[3].Value, []byte("x")) {
		t.Fatalf("row 3: got value %v, want %q", allRows[3].Value, "x")
	}
}

func TestWalReaderMissingFile(t *testing.T) {
	store := newMemoryStore(t)
	seedWalFiles(t, store)

	reader := openTestWalReader(t, store)

	files, err := reader.List(nil, nil)
	if err != nil {
		t.Fatalf("WalReader.List(nil, nil): %v", err)
	}
	for _, file := range files {
		defer file.Destroy()
	}

	if len(files) == 0 {
		t.Fatal("WalReader.List(nil, nil): got 0 files, want at least 1")
	}

	missingID := files[len(files)-1].Id() + 1000
	missing := reader.Get(missingID)
	defer missing.Destroy()

	if missing.Id() != missingID {
		t.Fatalf("WalReader.Get(missing): got id %d, want %d", missing.Id(), missingID)
	}

	if _, err := missing.Metadata(); err == nil {
		t.Fatal("WalFile.Metadata() for missing file: got nil error, want non-nil error")
	}
}

// Logging is process-global in the UniFFI bindings. Keep all Go logging
// assertions in this single in-process test because InitLogging succeeds only
// once per process.
func TestLoggingCallback(t *testing.T) {
	store := newMemoryStore(t)

	collector := &logCollector{}
	var callback slatedb.LogCallback = collector

	if err := slatedb.InitLogging(slatedb.LogLevelInfo, &callback); err != nil {
		t.Fatalf("InitLogging(first): %v", err)
	}

	err := slatedb.InitLogging(slatedb.LogLevelInfo, &callback)
	if !errors.Is(err, slatedb.ErrErrorInvalid) {
		t.Fatalf("InitLogging(second): got %v, want invalid error", err)
	}

	var invalidErr *slatedb.ErrorInvalid
	if !errors.As(err, &invalidErr) {
		t.Fatalf("InitLogging(second): expected *ErrorInvalid, got %T", err)
	}
	if invalidErr.Message != "logging already initialized" {
		t.Fatalf("InitLogging(second): message = %q, want %q", invalidErr.Message, "logging already initialized")
	}

	path := fmt.Sprintf("test-db-logging-%d", time.Now().UnixNano())
	builder := slatedb.NewDbBuilder(path, store)
	defer builder.Destroy()

	db, err := builder.Build()
	if err != nil {
		t.Fatalf("Build(): %v", err)
	}

	destroyed := false
	defer func() {
		if !destroyed {
			db.Destroy()
		}
	}()

	var openRecord slatedb.LogRecord
	waitUntil(t, 60*time.Second, 10*time.Millisecond, func() (bool, error) {
		record, found := collector.matchingRecord(func(record slatedb.LogRecord) bool {
			return record.Level == slatedb.LogLevelInfo &&
				strings.Contains(record.Message, "opening SlateDB database") &&
				strings.Contains(record.Message, path)
		})
		if !found {
			return false, nil
		}

		openRecord = record
		return true, nil
	})

	if openRecord.Target == "" {
		t.Fatal("open log target is empty")
	}
	if openRecord.ModulePath == nil || *openRecord.ModulePath == "" {
		t.Fatalf("open log module_path = %v, want non-empty value", openRecord.ModulePath)
	}
	if openRecord.File == nil || *openRecord.File == "" {
		t.Fatalf("open log file = %v, want non-empty value", openRecord.File)
	}
	if openRecord.Line == nil || *openRecord.Line == 0 {
		t.Fatalf("open log line = %v, want non-zero value", openRecord.Line)
	}

	if err := db.Shutdown(); err != nil {
		t.Fatalf("Shutdown(): %v", err)
	}

	db.Destroy()
	destroyed = true
}

func TestDefaultMetricsRecorderSnapshot(t *testing.T) {
	recorder := slatedb.NewDefaultMetricsRecorder()
	t.Cleanup(recorder.Destroy)

	counter := recorder.RegisterCounter("test.counter", "counter", nil)
	gauge := recorder.RegisterGauge("test.gauge", "gauge", nil)
	upDownCounter := recorder.RegisterUpDownCounter("test.up_down_counter", "up/down counter", nil)
	histogram := recorder.RegisterHistogram("test.histogram", "histogram", nil, []float64{1.0, 2.0})

	counter.Increment(3)
	gauge.Set(-7)
	upDownCounter.Increment(5)
	upDownCounter.Increment(-2)
	histogram.Record(1.5)
	histogram.Record(3.0)

	if got := recorder.MetricsByName("test.counter"); len(got) != 1 {
		t.Fatalf("MetricsByName(test.counter): got %d metrics, want 1", len(got))
	}

	counterMetric := recorder.MetricByNameAndLabels("test.counter", nil)
	if counterMetric == nil {
		t.Fatal("MetricByNameAndLabels(test.counter): got nil metric")
	}
	counterValue, ok := counterMetric.Value.(slatedb.MetricValueCounter)
	if !ok {
		t.Fatalf("MetricByNameAndLabels(test.counter): got %T, want MetricValueCounter", counterMetric.Value)
	}
	if counterValue.Field0 != 3 {
		t.Fatalf("counter metric value: got %d, want 3", counterValue.Field0)
	}

	histogramMetric := recorder.MetricByNameAndLabels("test.histogram", nil)
	if histogramMetric == nil {
		t.Fatal("MetricByNameAndLabels(test.histogram): got nil metric")
	}
	histogramValue, ok := histogramMetric.Value.(slatedb.MetricValueHistogram)
	if !ok {
		t.Fatalf("MetricByNameAndLabels(test.histogram): got %T, want MetricValueHistogram", histogramMetric.Value)
	}
	if histogramValue.Field0.Count != 2 {
		t.Fatalf("histogram count: got %d, want 2", histogramValue.Field0.Count)
	}
	if histogramValue.Field0.Sum != 4.5 {
		t.Fatalf("histogram sum: got %f, want 4.5", histogramValue.Field0.Sum)
	}

	if got := recorder.Snapshot(); len(got) < 4 {
		t.Fatalf("Snapshot(): got %d metrics, want at least 4", len(got))
	}
}

func TestDbBuilderWithCustomMetricsRecorder(t *testing.T) {
	store := newMemoryStore(t)
	recorder := newTestMetricsRecorder()
	handle := openTestDB(t, store, func(t *testing.T, builder *slatedb.DbBuilder) {
		t.Helper()
		if err := builder.WithMetricsRecorder(recorder); err != nil {
			t.Fatalf("WithMetricsRecorder(): %v", err)
		}
	})

	if _, err := handle.db.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("Put(k1): %v", err)
	}
	if _, err := handle.db.Put([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("Put(k2): %v", err)
	}

	value, ok := recorder.counterValue(dbWriteOpsMetricName, nil)
	if !ok {
		t.Fatalf("counter %q was never registered", dbWriteOpsMetricName)
	}
	if value != 2 {
		t.Fatalf("counter %q: got %d, want 2", dbWriteOpsMetricName, value)
	}
}

func TestDbReaderBuilderWithDefaultMetricsRecorder(t *testing.T) {
	store := newMemoryStore(t)
	writer := openTestDB(t, store, nil)

	if _, err := writer.db.Put([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Put(key1): %v", err)
	}
	if err := writer.db.Flush(); err != nil {
		t.Fatalf("Flush(): %v", err)
	}
	if err := writer.db.Shutdown(); err != nil {
		t.Fatalf("writer Shutdown(): %v", err)
	}
	writer.open = false

	recorder := slatedb.NewDefaultMetricsRecorder()
	t.Cleanup(recorder.Destroy)

	reader := openTestReader(t, store, func(t *testing.T, builder *slatedb.DbReaderBuilder) {
		t.Helper()
		if err := builder.WithMetricsRecorder(recorder); err != nil {
			t.Fatalf("WithMetricsRecorder(): %v", err)
		}
	})

	value, err := reader.reader.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get(key1): %v", err)
	}
	if value == nil || !bytes.Equal(*value, []byte("value1")) {
		t.Fatalf("Get(key1): got %v, want value1", value)
	}

	metric := recorder.MetricByNameAndLabels(dbRequestCountMetricName, []slatedb.MetricLabel{
		{Key: "op", Value: "get"},
	})
	if metric == nil {
		t.Fatalf("MetricByNameAndLabels(%q): got nil metric", dbRequestCountMetricName)
	}
	counterValue, ok := metric.Value.(slatedb.MetricValueCounter)
	if !ok {
		t.Fatalf("MetricByNameAndLabels(%q): got %T, want MetricValueCounter", dbRequestCountMetricName, metric.Value)
	}
	if counterValue.Field0 != 1 {
		t.Fatalf("counter %q: got %d, want 1", dbRequestCountMetricName, counterValue.Field0)
	}
}
