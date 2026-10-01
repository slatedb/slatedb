package slatedb_test

import (
	"testing"
	"time"

	slatedb "slatedb.io/slatedb-go/uniffi"
)

// A foreground Admin loop returns once its token is cancelled, whether the
// cancel came before the loop started or from another goroutine while it ran.
func TestAdminForegroundLoopsStopOnCancellation(t *testing.T) {
	store := newMemoryStore(t)
	dbHandle := openTestDB(t, store, nil)
	if _, err := dbHandle.db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put(): %v", err)
	}
	if err := dbHandle.db.Shutdown(); err != nil {
		t.Fatalf("Shutdown(): %v", err)
	}
	dbHandle.open = false

	admin := openTestAdmin(t, store, nil)

	loops := []struct {
		name string
		run  func(token *slatedb.CancellationToken) error
	}{
		{"RunGc", func(token *slatedb.CancellationToken) error { return admin.RunGc(token, nil) }},
		{"RunCompactor", func(token *slatedb.CancellationToken) error { return admin.RunCompactor(token, nil) }},
		{"RunCompactionWorker", func(token *slatedb.CancellationToken) error { return admin.RunCompactionWorker(token, nil) }},
	}

	for _, loop := range loops {
		t.Run(loop.name+"/cancelled before start", func(t *testing.T) {
			token := slatedb.NewCancellationToken()
			defer token.Destroy()
			token.Cancel()
			if !token.IsCancelled() {
				t.Fatal("IsCancelled() = false after Cancel()")
			}
			if err := loop.run(token); err != nil {
				t.Fatalf("%s(cancelled token): %v", loop.name, err)
			}
		})
		t.Run(loop.name+"/cancelled while running", func(t *testing.T) {
			token := slatedb.NewCancellationToken()
			defer token.Destroy()
			done := make(chan error, 1)
			go func() { done <- loop.run(token) }()
			select {
			case err := <-done:
				t.Fatalf("%s returned before Cancel(): %v", loop.name, err)
			case <-time.After(300 * time.Millisecond):
			}
			token.Cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("%s after Cancel(): %v", loop.name, err)
				}
			case <-time.After(15 * time.Second):
				t.Fatalf("%s did not return within 15 s of Cancel()", loop.name)
			}
		})
	}
}

// The compactor schedules a compaction over two L0 SSTs only because the
// options it is given lower the scheduler's minimum from four sources to two;
// a loop that fell back to the engine's defaults would schedule nothing.
func TestAdminRunCompactorUsesItsOptions(t *testing.T) {
	store := newMemoryStore(t)
	settings := slatedb.SettingsDefault()
	defer settings.Destroy()
	if err := settings.Set("compactor_options", "null"); err != nil {
		t.Fatalf("Settings.Set(compactor_options): %v", err)
	}
	dbHandle := openTestDB(t, store, func(t *testing.T, builder *slatedb.DbBuilder) {
		if err := builder.WithSettings(settings); err != nil {
			t.Fatalf("WithSettings(): %v", err)
		}
	})
	for i := 0; i < 2; i++ {
		if _, err := dbHandle.db.Put([]byte{byte(i)}, []byte{byte(i)}); err != nil {
			t.Fatalf("Put(): %v", err)
		}
		if err := dbHandle.db.FlushWithOptions(slatedb.FlushOptions{FlushType: slatedb.FlushTypeMemTable}); err != nil {
			t.Fatalf("FlushWithOptions(MemTable): %v", err)
		}
	}
	if err := dbHandle.db.Shutdown(); err != nil {
		t.Fatalf("Shutdown(): %v", err)
	}
	dbHandle.open = false
	admin := openTestAdmin(t, store, nil)

	options := slatedb.CompactorOptions{
		PollIntervalMs:            50,
		ManifestUpdateTimeoutMs:   300_000,
		MaxConcurrentCompactions:  4,
		EnableTrivialMove:         false,
		SchedulerOptions:          map[string]string{"min_compaction_sources": "2"},
		Worker:                    nil,
		CommitCompactedIntervalMs: 1_000,
		CheckpointLifetimeMs:      900_000,
		WorkerHeartbeatTimeoutMs:  30_000,
		ObjectStoreMaxRetries:     nil,
	}
	token := slatedb.NewCancellationToken()
	defer token.Destroy()
	done := make(chan error, 1)
	go func() { done <- admin.RunCompactor(token, &options) }()

	deadline := time.Now().Add(30 * time.Second)
	for {
		compactions, err := admin.ReadCompactions(nil)
		if err != nil {
			t.Fatalf("ReadCompactions(): %v", err)
		}
		if compactions != nil && len(compactions.RecentCompactions) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the compactor scheduled nothing over two L0 SSTs")
		}
		select {
		case err := <-done:
			t.Fatalf("RunCompactor returned before Cancel(): %v", err)
		case <-time.After(50 * time.Millisecond):
		}
	}

	token.Cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunCompactor after Cancel(): %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("RunCompactor did not return within 15 s of Cancel()")
	}
}
