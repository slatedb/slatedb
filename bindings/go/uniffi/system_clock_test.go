package slatedb_test

import (
	"errors"
	"testing"
	"time"

	slatedb "slatedb.io/slatedb-go/uniffi"
)

// A database built with a mock clock stamps its writes from that clock
// alone: time moves only when the test advances it.
func TestDbFollowsTheInstalledSystemClock(t *testing.T) {
	store := newMemoryStore(t)
	clock := slatedb.SystemClockMock(1_000_000)
	defer clock.Destroy()
	if !clock.IsMock() || clock.NowMillis() != 1_000_000 {
		t.Fatalf("SystemClockMock(1_000_000): IsMock=%v NowMillis=%d", clock.IsMock(), clock.NowMillis())
	}

	dbHandle := openTestDB(t, store, func(t *testing.T, builder *slatedb.DbBuilder) {
		t.Helper()
		if err := builder.WithSystemClock(clock); err != nil {
			t.Fatalf("DbBuilder.WithSystemClock(): %v", err)
		}
	})

	first, err := dbHandle.db.Put([]byte("k"), []byte("v"))
	if err != nil {
		t.Fatalf("Put(): %v", err)
	}
	defer first.Destroy()
	if got := first.CreateTs(); got != 1_000_000 {
		t.Fatalf("CreateTs() = %d, want the mock clock's 1000000", got)
	}
	time.Sleep(20 * time.Millisecond)
	second, err := dbHandle.db.Put([]byte("k"), []byte("w"))
	if err != nil {
		t.Fatalf("Put(): %v", err)
	}
	defer second.Destroy()
	if got := second.CreateTs(); got != 1_000_000 {
		t.Fatalf("CreateTs() after 20 ms of wall time = %d, want 1000000: the mock clock moved on its own", got)
	}
	if err := clock.Advance(500); err != nil {
		t.Fatalf("Advance(500): %v", err)
	}
	if got := clock.NowMillis(); got != 1_000_500 {
		t.Fatalf("NowMillis() after Advance(500) = %d, want 1000500", got)
	}
	third, err := dbHandle.db.Put([]byte("k"), []byte("x"))
	if err != nil {
		t.Fatalf("Put(): %v", err)
	}
	defer third.Destroy()
	if got := third.CreateTs(); got != 1_000_500 {
		t.Fatalf("CreateTs() after Advance(500) = %d, want 1000500", got)
	}
	if err := dbHandle.db.Shutdown(); err != nil {
		t.Fatalf("Shutdown(): %v", err)
	}
	dbHandle.open = false
}

// The default clock cannot be driven, and a consumed builder refuses a clock.
func TestSystemClockDefaultRefusesToBeDriven(t *testing.T) {
	clock := slatedb.SystemClockDefault()
	defer clock.Destroy()
	if clock.IsMock() {
		t.Fatal("SystemClockDefault().IsMock() = true")
	}
	if err := clock.Advance(1); !errors.Is(err, slatedb.ErrErrorInvalid) {
		t.Fatalf("Advance() on the default clock: got %v, want an invalid error", err)
	}
	if err := clock.Set(1); err == nil {
		t.Fatal("Set() on the default clock succeeded")
	}
	before := clock.NowMillis()
	time.Sleep(5 * time.Millisecond)
	if clock.NowMillis() < before {
		t.Fatal("the default clock went backwards")
	}

	store := newMemoryStore(t)
	reader := slatedb.NewDbReaderBuilder(testDBPath, store)
	defer reader.Destroy()
	if err := reader.WithSystemClock(clock); err != nil {
		t.Fatalf("DbReaderBuilder.WithSystemClock(): %v", err)
	}
	admin := slatedb.NewAdminBuilder(testDBPath, store)
	defer admin.Destroy()
	if err := admin.WithSystemClock(clock); err != nil {
		t.Fatalf("AdminBuilder.WithSystemClock(): %v", err)
	}
	built, err := admin.Build()
	if err != nil {
		t.Fatalf("AdminBuilder.Build(): %v", err)
	}
	built.Destroy()
	if err := admin.WithSystemClock(clock); err == nil {
		t.Fatal("a consumed AdminBuilder accepted a clock")
	}
}
