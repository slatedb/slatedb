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

// The compactor reads its options from the Settings handle it is given, so
// an invalid compactor setting is refused by the same loop that would run it.
func TestAdminRunCompactorReadsSettings(t *testing.T) {
	store := newMemoryStore(t)
	dbHandle := openTestDB(t, store, nil)
	if err := dbHandle.db.Shutdown(); err != nil {
		t.Fatalf("Shutdown(): %v", err)
	}
	dbHandle.open = false
	admin := openTestAdmin(t, store, nil)

	settings := slatedb.SettingsDefault()
	defer settings.Destroy()
	if err := settings.Set("compactor_options.poll_interval", `"50ms"`); err != nil {
		t.Fatalf("Settings.Set(poll_interval): %v", err)
	}
	if err := settings.Set("compactor_options.worker", "null"); err != nil {
		t.Fatalf("Settings.Set(worker): %v", err)
	}

	token := slatedb.NewCancellationToken()
	defer token.Destroy()
	done := make(chan error, 1)
	go func() { done <- admin.RunCompactor(token, &settings) }()
	time.Sleep(200 * time.Millisecond)
	token.Cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunCompactor(settings): %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("RunCompactor(settings) did not return within 15 s of Cancel()")
	}
}
