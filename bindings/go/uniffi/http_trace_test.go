package slatedb_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	slatedb "slatedb.io/slatedb-go/uniffi"
)

func TestHttpTraceCaptureCountsNativeTransport(t *testing.T) {
	for _, capacity := range []uint32{64, 0} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			var calls atomic.Uint64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte("private-response"))
			}))
			defer server.Close()
			capture := slatedb.NewHttpTraceCapture(capacity)
			defer capture.Destroy()
			builder := slatedb.NewObjectStoreBuilder(slatedb.ObjectStoreTypeS3)
			defer builder.Destroy()
			for key, value := range map[string]string{
				"bucket": "test-bucket", "region": "test-region",
				"access_key_id": "test-key", "secret_access_key": "test-secret",
				"allow_http": "true", "endpoint": server.URL,
			} {
				if err := builder.WithConfig(key, value); err != nil {
					t.Fatal(err)
				}
			}
			if err := builder.WithHttpTrace(capture); err != nil {
				t.Fatal(err)
			}
			store, err := builder.Build()
			if err != nil {
				t.Fatal(err)
			}
			defer store.Destroy()
			dbBuilder := slatedb.NewDbBuilder("private-path", store)
			defer dbBuilder.Destroy()
			settings := slatedb.SettingsDefault()
			defer settings.Destroy()
			if err := settings.Set("object_store_max_retries", "0"); err != nil {
				t.Fatal(err)
			}
			if err := dbBuilder.WithSettings(settings); err != nil {
				t.Fatal(err)
			}
			db, err := dbBuilder.Build()
			if err == nil {
				_ = db.Shutdown()
				db.Destroy()
				t.Fatal("database opened despite forbidden storage")
			}
			snapshot := capture.Snapshot()
			if calls.Load() == 0 || snapshot.OpenCalls != 0 {
				t.Fatalf("calls=%d open=%d", calls.Load(), snapshot.OpenCalls)
			}
			if capacity == 0 {
				if snapshot.CompletedCallCount != nil || snapshot.DroppedCalls != calls.Load() || len(snapshot.Records) != 0 {
					t.Fatalf("overflow: %+v", snapshot)
				}
				return
			}
			if snapshot.CompletedCallCount == nil || *snapshot.CompletedCallCount != calls.Load() || snapshot.DroppedCalls != 0 || uint64(len(snapshot.Records)) != calls.Load() {
				t.Fatalf("incomplete capture: %+v, server calls=%d", snapshot, calls.Load())
			}
			for _, record := range snapshot.Records {
				if record.Status == nil || *record.Status != http.StatusForbidden || record.Outcome != slatedb.HttpCallOutcomeComplete {
					t.Fatalf("response: %+v", record)
				}
				if record.HeadersUs == nil || record.StartUs > *record.HeadersUs || *record.HeadersUs > record.FinishUs || record.ConsumedBytes != uint64(len("private-response")) {
					t.Fatalf("body interval: %+v", record)
				}
			}
			encoded, err := json.Marshal(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			for _, private := range []string{server.URL, "private-path", "private-response", "test-secret"} {
				if strings.Contains(string(encoded), private) {
					t.Fatalf("capture contains %q", private)
				}
			}
		})
	}
}
