package slatedb_test

import (
	"testing"

	slatedb "slatedb.io/slatedb-go/uniffi"
)

// A write is committed when Put returns, before it is durable; durability
// never runs ahead of the commit, and a reader's committed seq is its durable seq.
func TestStatusCarriesTheCommittedSeq(t *testing.T) {
	store := newMemoryStore(t)
	dbHandle := openTestDB(t, store, nil)

	write, err := dbHandle.db.Put([]byte("k"), []byte("v"))
	if err != nil {
		t.Fatalf("Put(): %v", err)
	}
	status := dbHandle.db.Status()
	if status.CommittedSeq != write.Seqnum() {
		t.Fatalf("Status().CommittedSeq = %d after Put, want %d", status.CommittedSeq, write.Seqnum())
	}
	if status.DurableSeq > status.CommittedSeq {
		t.Fatalf("Status().DurableSeq = %d exceeds CommittedSeq = %d", status.DurableSeq, status.CommittedSeq)
	}

	if err := write.AwaitDurable(); err != nil {
		t.Fatalf("WriteHandle.AwaitDurable(): %v", err)
	}
	status = dbHandle.db.Status()
	if status.DurableSeq < write.Seqnum() || status.CommittedSeq < status.DurableSeq {
		t.Fatalf("Status() after AwaitDurable = durable %d, committed %d; want both >= %d", status.DurableSeq, status.CommittedSeq, write.Seqnum())
	}

	reader := openTestReader(t, store, func(t *testing.T, builder *slatedb.DbReaderBuilder) {
		t.Helper()
		if err := builder.WithReaderMode(slatedb.ReaderModeFollowLatest{}); err != nil {
			t.Fatalf("WithReaderMode(): %v", err)
		}
	})
	readerStatus := reader.reader.Status()
	if readerStatus.CommittedSeq != readerStatus.DurableSeq {
		t.Fatalf("reader Status() = committed %d, durable %d; want equal", readerStatus.CommittedSeq, readerStatus.DurableSeq)
	}
}
