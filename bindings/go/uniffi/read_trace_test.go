package slatedb_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"

	slatedb "slatedb.io/slatedb-go/uniffi"
)

type readTraceLogs struct{ low, total atomic.Uint64 }

func (l *readTraceLogs) Log(record slatedb.LogRecord) {
	l.total.Add(1)
	if record.Level != slatedb.LogLevelWarn && record.Level != slatedb.LogLevelError {
		l.low.Add(1)
	}
}

func TestReadTraceCapture(t *testing.T) {
	mode := os.Getenv("SLATEDB_READ_TRACE_CASE")
	if mode == "" {
		for _, child := range []string{"warn", "off", "off-fmt", "late", "off-filter"} {
			t.Run(child, func(t *testing.T) {
				command := exec.Command(os.Args[0], "-test.run=^TestReadTraceCapture$", "-test.v", "-test.timeout=45s")
				command.Env = append(os.Environ(), "SLATEDB_READ_TRACE_CASE="+child)
				output, err := command.CombinedOutput()
				if err != nil {
					t.Fatalf("%v\n%s", err, output)
				}
				t.Log(string(output))
			})
		}
		return
	}
	capture := slatedb.NewHttpTraceCapture(256)
	defer capture.Destroy()
	if mode == "late" {
		if err := slatedb.InitLogging(slatedb.LogLevelOff, nil); err != nil {
			t.Fatal(err)
		}
		if err := slatedb.InitLoggingWithTrace(slatedb.LogLevelOff, nil, capture); !errors.Is(err, slatedb.ErrErrorInvalid) {
			t.Fatalf("late capture: %v", err)
		}
		return
	}
	logs := &readTraceLogs{}
	var sink slatedb.LogCallback = logs
	callback := &sink
	level := slatedb.LogLevelOff
	if mode == "warn" {
		level = slatedb.LogLevelWarn
	}
	if mode == "off-fmt" {
		callback = nil
	}
	if err := slatedb.InitLoggingWithTrace(level, callback, capture); err != nil {
		t.Fatal(err)
	}
	if err := slatedb.InitLogging(level, callback); !errors.Is(err, slatedb.ErrErrorInvalid) {
		t.Fatalf("duplicate initializer: %v", err)
	}
	if mode == "off-filter" {
		readTraceFilter(t, capture, logs)
		return
	}
	store := newMemoryStore(t)
	configure := func(t *testing.T, builder *slatedb.DbBuilder) {
		if err := builder.WithDbCacheDisabled(); err != nil {
			t.Fatal(err)
		}
	}
	writer := openTestDB(t, store, configure)
	payload := bytes.Repeat([]byte("private-row-content"), 512)
	for i := 0; i < 64; i++ {
		if _, err := writer.db.Put([]byte(fmt.Sprintf("key-%03d", i)), payload); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.db.FlushWithOptions(slatedb.FlushOptions{FlushType: slatedb.FlushTypeMemTable}); err != nil {
		t.Fatal(err)
	}
	if err := writer.db.Shutdown(); err != nil {
		t.Fatal(err)
	}
	writer.open = false
	reader := openTestDB(t, store, configure)
	value, err := reader.db.GetWithOptions([]byte("key-000"), slatedb.ReadOptions{
		DurabilityFilter: slatedb.DurabilityLevelRemote,
		TracingOptions:   &slatedb.TracingOptions{TraceId: "cold-get"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if value == nil || !bytes.Equal(*value, payload) {
		t.Fatalf("cold get: %v", value)
	}
	iter, err := reader.db.ScanWithOptions(slatedb.KeyRange{}, slatedb.ScanOptions{
		DurabilityFilter: slatedb.DurabilityLevelRemote,
		ReadAheadBytes:   1, MaxFetchTasks: 1,
		TracingOptions: &slatedb.TracingOptions{TraceId: "paged-scan"},
	})
	if err != nil {
		t.Fatal(err)
	}
	destroyed := false
	defer func() {
		if !destroyed {
			iter.Destroy()
		}
	}()
	first, err := iter.NextBatch(1)
	if err != nil || len(first) != 1 {
		t.Fatalf("first scan batch: %v rows=%d", err, len(first))
	}
	initialBlocks := map[uint64]bool{}
	for _, span := range capture.Snapshot().ReadSpans {
		if span.Kind == slatedb.ReadSpanKindReadBlocks {
			initialBlocks[span.Span] = true
		}
	}
	rows := len(first)
	for {
		batch, err := iter.NextBatch(7)
		if err != nil {
			t.Fatal(err)
		}
		rows += len(batch)
		if len(batch) == 0 {
			break
		}
	}
	if rows != 64 {
		t.Fatalf("rows=%d", rows)
	}
	atEOF := capture.Snapshot()
	if atEOF.OpenReadSpans == 0 {
		t.Fatalf("iterator lifetime: %+v", atEOF)
	}
	iter.Destroy()
	destroyed = true
	snapshot := capture.Snapshot()
	if snapshot.OpenReadSpans != 0 || snapshot.DroppedReadSpans != 0 {
		t.Fatalf("incomplete: %+v", snapshot)
	}
	if snapshot.DroppedReadDependencies != 0 || snapshot.UnresolvedReadDependencies != 0 {
		t.Fatalf("selected dependencies lost: %+v", snapshot)
	}
	spans := map[uint64]slatedb.ReadSpanRecord{}
	roots := map[string]uint64{}
	for _, span := range snapshot.ReadSpans {
		spans[span.Span] = span
		if span.StartUs > span.FinishUs {
			t.Fatalf("interval: %+v", span)
		}
		if span.TraceId != nil {
			roots[*span.TraceId] = span.Span
		}
	}
	if roots["cold-get"] == 0 || roots["paged-scan"] == 0 {
		t.Fatalf("roots=%v", roots)
	}
	dependencies := map[uint64]uint64{}
	for _, dependency := range snapshot.ReadDependencies {
		predecessor, predecessorOK := spans[dependency.Predecessor]
		dependent, dependentOK := spans[dependency.Dependent]
		if !predecessorOK || !dependentOK || predecessor.Kind != slatedb.ReadSpanKindReadIndex || dependent.Kind != slatedb.ReadSpanKindReadBlocks {
			t.Fatalf("selected dependency endpoints: %+v", dependency)
		}
		if predecessor.ParentSpan == nil || dependent.ParentSpan == nil || *predecessor.ParentSpan != *dependent.ParentSpan {
			t.Fatalf("selected dependency ownership: %+v -> %+v", predecessor, dependent)
		}
		if _, duplicate := dependencies[dependency.Dependent]; duplicate {
			t.Fatalf("duplicate block dependency: %+v", dependency)
		}
		dependencies[dependency.Dependent] = dependency.Predecessor
	}
	laterBlocks := 0
	for _, span := range snapshot.ReadSpans {
		if span.Kind == slatedb.ReadSpanKindReadBlocks && dependencies[span.Span] == 0 {
			t.Fatalf("block without index dependency: %+v", span)
		}
		if span.Kind == slatedb.ReadSpanKindReadBlocks && span.ParentSpan != nil && *span.ParentSpan == roots["paged-scan"] && !initialBlocks[span.Span] {
			laterBlocks++
		}
	}
	if laterBlocks < 2 {
		t.Fatalf("later scan block spans=%d", laterBlocks)
	}
	if logs.low.Load() != 0 {
		t.Fatalf("low-level log callbacks=%d", logs.low.Load())
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{"private-row-content", "key-000"} {
		if strings.Contains(string(encoded), private) {
			t.Fatalf("capture includes %q", private)
		}
	}
}

func readTraceFilter(t *testing.T, capture *slatedb.HttpTraceCapture, logs *readTraceLogs) {
	t.Helper()
	store := newMemoryStore(t)
	configure := func(t *testing.T, builder *slatedb.DbBuilder) {
		if err := builder.WithDbCacheDisabled(); err != nil {
			t.Fatal(err)
		}
	}
	writer := openTestDB(t, store, configure)
	batch := slatedb.NewWriteBatch()
	defer batch.Destroy()
	payload := []byte("private-row-content")
	for i := 0; i < 1000; i++ {
		if err := batch.Put([]byte(fmt.Sprintf("key-%04d", i)), payload); err != nil {
			t.Fatal(err)
		}
	}
	write, err := writer.db.Write(batch)
	if err != nil {
		t.Fatal(err)
	}
	write.Destroy()
	if err := writer.db.FlushWithOptions(slatedb.FlushOptions{FlushType: slatedb.FlushTypeMemTable}); err != nil {
		t.Fatal(err)
	}
	if err := writer.db.Shutdown(); err != nil {
		t.Fatal(err)
	}
	writer.open = false
	reader := openTestDB(t, store, configure)
	value, err := reader.db.GetWithOptions([]byte("key-0000"), slatedb.ReadOptions{
		DurabilityFilter: slatedb.DurabilityLevelRemote,
		TracingOptions:   &slatedb.TracingOptions{TraceId: "cold-filter"},
	})
	if err != nil || value == nil || !bytes.Equal(*value, payload) {
		t.Fatalf("filter get: value=%v err=%v", value, err)
	}
	snapshot := capture.Snapshot()
	if snapshot.OpenReadSpans != 0 || snapshot.DroppedReadSpans != 0 || len(snapshot.ReadSpans) > 256 ||
		snapshot.DroppedReadDependencies != 0 || snapshot.UnresolvedReadDependencies != 0 ||
		snapshot.OpenCalls != 0 || snapshot.DroppedCalls != 0 {
		t.Fatalf("incomplete filter capture: %+v", snapshot)
	}
	spans := map[slatedb.ReadSpanKind]slatedb.ReadSpanRecord{}
	for _, span := range snapshot.ReadSpans {
		if span.StartUs > span.FinishUs {
			t.Fatalf("filter interval: %+v", span)
		}
		if span.Kind == slatedb.ReadSpanKindMemtable {
			continue
		}
		if _, duplicate := spans[span.Kind]; duplicate {
			t.Fatalf("duplicate filter stage: %+v", span)
		}
		spans[span.Kind] = span
	}
	root := spans[slatedb.ReadSpanKindRead]
	if root.Span == 0 || root.TraceId == nil || *root.TraceId != "cold-filter" {
		t.Fatalf("filter root: %+v", root)
	}
	stages := []slatedb.ReadSpanKind{slatedb.ReadSpanKindReadFilters, slatedb.ReadSpanKindEvaluateFilter, slatedb.ReadSpanKindReadIndex, slatedb.ReadSpanKindReadBlocks}
	for _, kind := range stages {
		span := spans[kind]
		if span.Span == 0 || span.ParentSpan == nil || *span.ParentSpan != root.Span {
			t.Fatalf("filter stage ownership: kind=%v span=%+v", kind, span)
		}
	}
	dependencies := map[uint64]uint64{}
	for _, dependency := range snapshot.ReadDependencies {
		if _, duplicate := dependencies[dependency.Dependent]; duplicate {
			t.Fatalf("duplicate filter dependency: %+v", dependency)
		}
		dependencies[dependency.Dependent] = dependency.Predecessor
	}
	if len(dependencies) != len(stages)-1 {
		t.Fatalf("filter dependency chain: %+v", snapshot.ReadDependencies)
	}
	for i := 1; i < len(stages); i++ {
		if dependencies[spans[stages[i]].Span] != spans[stages[i-1]].Span {
			t.Fatalf("filter dependency chain: %+v", snapshot.ReadDependencies)
		}
	}
	if logs.total.Load() != 0 {
		t.Fatalf("log callbacks with logging off=%d", logs.total.Load())
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{"private-row-content", "key-0000"} {
		if strings.Contains(string(encoded), private) {
			t.Fatalf("filter capture includes %q", private)
		}
	}
}
