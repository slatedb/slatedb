package slatedb_test

import (
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"slatedb.io/slatedb-go"
)

var _ = Describe("Configuration", func() {
	Describe("SettingsDefault", func() {
		It("should create valid default settings", func() {
			settings, err := slatedb.SettingsDefault()
			Expect(err).ToNot(HaveOccurred())
			Expect(settings).ToNot(BeNil())
			Expect(settings.FlushInterval).ToNot(BeEmpty())
			Expect(settings.L0SstSizeBytes).To(BeNumerically(">", 0))
		})
	})

	Describe("MergeSettings", func() {
		var base, override *slatedb.Settings

		BeforeEach(func() {
			base = &slatedb.Settings{
				FlushInterval:  "100ms",
				L0SstSizeBytes: 1048576,
				MinFilterKeys:  1000,
				CompactorOptions: &slatedb.CompactorOptions{
					PollInterval:             "1s",
					ManifestUpdateTimeout:    "30s",
					MaxSstSize:               67108864,
					MaxConcurrentCompactions: 2,
				},
				ObjectStoreCacheOptions: &slatedb.ObjectStoreCacheOptions{
					MaxCacheSizeBytes: 134217728,
					RootFolder:        "/tmp/base",
				},
			}
		})

		It("should merge with zero-value semantics", func() {
			override = &slatedb.Settings{
				FlushInterval:  "200ms", // Override
				L0SstSizeBytes: 0,       // Zero - preserve base
				MinFilterKeys:  2000,    // Override
			}

			merged := slatedb.MergeSettings(base, override)

			Expect(merged.FlushInterval).To(Equal("200ms"))          // Overridden
			Expect(merged.L0SstSizeBytes).To(Equal(uint64(1048576))) // Preserved from base
			Expect(merged.MinFilterKeys).To(Equal(uint32(2000)))     // Overridden
		})

		It("should merge nested structs field-by-field", func() {
			override = &slatedb.Settings{
				CompactorOptions: &slatedb.CompactorOptions{
					PollInterval: "2s", // Override only this field
					MaxSstSize:   134217728,
				},
				ObjectStoreCacheOptions: &slatedb.ObjectStoreCacheOptions{
					RootFolder: "/tmp/override", // Override only this field
				},
			}

			merged := slatedb.MergeSettings(base, override)

			// CompactorOptions: some fields overridden, others preserved
			Expect(merged.CompactorOptions.PollInterval).To(Equal("2s"))
			Expect(merged.CompactorOptions.ManifestUpdateTimeout).To(Equal("30s")) // Preserved
			Expect(merged.CompactorOptions.MaxSstSize).To(Equal(uint64(134217728)))
			Expect(merged.CompactorOptions.MaxConcurrentCompactions).To(Equal(uint32(2))) // Preserved

			// ObjectStoreCacheOptions: partial override
			Expect(merged.ObjectStoreCacheOptions.RootFolder).To(Equal("/tmp/override"))
			Expect(merged.ObjectStoreCacheOptions.MaxCacheSizeBytes).To(Equal(uint64(134217728))) // Preserved
		})

		It("should handle nil cases", func() {
			Expect(slatedb.MergeSettings(nil, base)).To(Equal(base))
			Expect(slatedb.MergeSettings(base, nil)).To(Equal(base))
			Expect(slatedb.MergeSettings(nil, nil)).ToNot(BeNil())
		})
	})

	Describe("GarbageCollectorOptions merging", func() {
		It("should merge nested GC options correctly", func() {
			base := &slatedb.Settings{
				GarbageCollectorOptions: &slatedb.GarbageCollectorOptions{
					Manifest: &slatedb.GarbageCollectorDirectoryOptions{
						Interval: "24h",
						MinAge:   "1h",
					},
				},
			}

			override := &slatedb.Settings{
				GarbageCollectorOptions: &slatedb.GarbageCollectorOptions{
					Manifest: &slatedb.GarbageCollectorDirectoryOptions{
						Interval: "48h",
						MinAge:   "1h",
					},
					Compacted: &slatedb.GarbageCollectorDirectoryOptions{
						Interval: "6h",
						MinAge:   "15m",
					},
				},
			}

			merged := slatedb.MergeSettings(base, override)

			Expect(merged.GarbageCollectorOptions.Manifest.Interval).To(Equal("48h"))
			Expect(merged.GarbageCollectorOptions.Manifest.MinAge).To(Equal("1h"))
			Expect(merged.GarbageCollectorOptions.Compacted.Interval).To(Equal("6h"))
			Expect(merged.GarbageCollectorOptions.Compacted.MinAge).To(Equal("15m"))
		})
	})

	Describe("JSON serialization", func() {
		It("should serialize and deserialize correctly", func() {
			original := &slatedb.Settings{
				FlushInterval:  "250ms",
				L0SstSizeBytes: 2097152,
				MinFilterKeys:  1500,
			}

			jsonData, err := json.Marshal(original)
			Expect(err).ToNot(HaveOccurred())

			var deserialized slatedb.Settings
			err = json.Unmarshal(jsonData, &deserialized)
			Expect(err).ToNot(HaveOccurred())

			Expect(deserialized.FlushInterval).To(Equal(original.FlushInterval))
			Expect(deserialized.L0SstSizeBytes).To(Equal(original.L0SstSizeBytes))
			Expect(deserialized.MinFilterKeys).To(Equal(original.MinFilterKeys))
		})
	})
})
