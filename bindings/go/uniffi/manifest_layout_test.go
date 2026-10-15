package slatedb_test

import (
	"testing"

	slatedb "slatedb.io/slatedb-go/uniffi"
)

type manifestLayoutExtractor struct {
	fixedThreeByteSegmentExtractor
	name string
}

func (e manifestLayoutExtractor) Name() string { return e.name }

func TestManifestReportsThePersistedSegmentLayout(t *testing.T) {
	for _, name := range []string{"", "othermemory/segments/v1", "future/segments/v2"} {
		t.Run(name, func(t *testing.T) {
			store := newMemoryStore(t)
			handle := openTestDB(t, store, func(t *testing.T, builder *slatedb.DbBuilder) {
				if name != "" {
					if err := builder.WithSegmentExtractor(manifestLayoutExtractor{name: name}); err != nil {
						t.Fatal(err)
					}
				}
			})
			if err := handle.db.Shutdown(); err != nil {
				t.Fatal(err)
			}
			handle.open = false
			admin := openTestAdmin(t, store, nil)
			manifest, err := admin.ReadManifest(nil)
			if err != nil || manifest == nil {
				t.Fatalf("read manifest = %v, %v", manifest, err)
			}
			checkLayout := func(m slatedb.VersionedManifest) {
				t.Helper()
				if name == "" {
					if m.SegmentExtractorName != nil {
						t.Fatalf("pre-layout manifest reports %q", *m.SegmentExtractorName)
					}
					return
				}
				if m.SegmentExtractorName == nil || *m.SegmentExtractorName != name {
					t.Fatalf("manifest %d layout = %v, want %q", m.Id, m.SegmentExtractorName, name)
				}
			}
			checkLayout(*manifest)
			versions, err := admin.ListManifests(nil, nil)
			if err != nil || len(versions) == 0 {
				t.Fatalf("list manifests = %v, %v", versions, err)
			}
			for _, version := range versions {
				checkLayout(version)
			}
		})
	}
}
