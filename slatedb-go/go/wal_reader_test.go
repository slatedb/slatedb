package slatedb_test

import (
	"io"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"slatedb.io/slatedb-go"
)

var _ = Describe("WalReader", func() {
	var (
		tmpDir  string
		envFile string
	)

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "slatedb_wal_test_*")
		Expect(err).NotTo(HaveOccurred())

		envFile, err = createEnvFile(tmpDir)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(os.RemoveAll(tmpDir)).NotTo(HaveOccurred())
	})

	seedDB := func(path, envFile string) {
		db, err := slatedb.Open(path, slatedb.WithEnvFile[slatedb.DbConfig](envFile))
		Expect(err).NotTo(HaveOccurred())
		_, err = db.Put([]byte("k1"), []byte("v1"))
		Expect(err).NotTo(HaveOccurred())
		_, err = db.Put([]byte("k2"), []byte("v2"))
		Expect(err).NotTo(HaveOccurred())
		Expect(db.Flush()).NotTo(HaveOccurred())
		Expect(db.Close()).NotTo(HaveOccurred())
	}

	Describe("NewWalReader", func() {
		It("should open a reader for a seeded database", func() {
			seedDB(tmpDir, envFile)

			reader, err := slatedb.NewWalReader(tmpDir, slatedb.WithEnvFile[slatedb.WalReaderConfig](envFile))
			Expect(err).NotTo(HaveOccurred())
			Expect(reader).NotTo(BeNil())
			Expect(reader.Close()).NotTo(HaveOccurred())
		})
	})

	Describe("List and Iterate", func() {
		It("should list WAL files and iterate their entries", func() {
			seedDB(tmpDir, envFile)

			reader, err := slatedb.NewWalReader(tmpDir, slatedb.WithEnvFile[slatedb.WalReaderConfig](envFile))
			Expect(err).NotTo(HaveOccurred())
			defer reader.Close()

			files, err := reader.List(nil, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(files).NotTo(BeEmpty())

			var allKeys [][]byte
			for _, f := range files {
				iter, err := f.Iterator()
				Expect(err).NotTo(HaveOccurred())

				for {
					entry, err := iter.Next()
					if err == io.EOF {
						break
					}
					Expect(err).NotTo(HaveOccurred())
					allKeys = append(allKeys, entry.Key)
				}

				Expect(iter.Close()).NotTo(HaveOccurred())
				Expect(f.Close()).NotTo(HaveOccurred())
			}

			Expect(allKeys).To(ContainElement([]byte("k1")))
			Expect(allKeys).To(ContainElement([]byte("k2")))
		})
	})

	Describe("Get and NextFile", func() {
		It("should get a file by ID and navigate to the next file", func() {
			reader, err := slatedb.NewWalReader(tmpDir, slatedb.WithEnvFile[slatedb.WalReaderConfig](envFile))
			Expect(err).NotTo(HaveOccurred())
			defer reader.Close()

			file, err := reader.Get(42)
			Expect(err).NotTo(HaveOccurred())
			Expect(file).NotTo(BeNil())

			id, err := file.ID()
			Expect(err).NotTo(HaveOccurred())
			Expect(id).To(Equal(uint64(42)))

			nextID, err := file.NextID()
			Expect(err).NotTo(HaveOccurred())
			Expect(nextID).To(Equal(uint64(43)))

			nextFile, err := file.NextFile()
			Expect(err).NotTo(HaveOccurred())
			Expect(nextFile).NotTo(BeNil())

			nfID, err := nextFile.ID()
			Expect(err).NotTo(HaveOccurred())
			Expect(nfID).To(Equal(uint64(43)))

			Expect(nextFile.Close()).NotTo(HaveOccurred())
			Expect(file.Close()).NotTo(HaveOccurred())
		})
	})

	Describe("Metadata", func() {
		It("should return metadata for a WAL file", func() {
			seedDB(tmpDir, envFile)

			reader, err := slatedb.NewWalReader(tmpDir, slatedb.WithEnvFile[slatedb.WalReaderConfig](envFile))
			Expect(err).NotTo(HaveOccurred())
			defer reader.Close()

			files, err := reader.List(nil, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(files).NotTo(BeEmpty())

			meta, err := files[0].Metadata()
			Expect(err).NotTo(HaveOccurred())
			Expect(meta.SizeBytes).To(BeNumerically(">", 0))
			Expect(meta.Location).NotTo(BeEmpty())
			Expect(meta.LastModified.IsZero()).To(BeFalse())

			for _, f := range files {
				Expect(f.Close()).NotTo(HaveOccurred())
			}
		})
	})

	Describe("Entry Kinds", func() {
		It("should read value and tombstone entries", func() {
			db, err := slatedb.Open(tmpDir, slatedb.WithEnvFile[slatedb.DbConfig](envFile))
			Expect(err).NotTo(HaveOccurred())

			_, err = db.Put([]byte("k_val"), []byte("hello"))
			Expect(err).NotTo(HaveOccurred())
			_, err = db.Delete([]byte("k_tomb"))
			Expect(err).NotTo(HaveOccurred())
			Expect(db.Flush()).NotTo(HaveOccurred())
			Expect(db.Close()).NotTo(HaveOccurred())

			reader, err := slatedb.NewWalReader(tmpDir, slatedb.WithEnvFile[slatedb.WalReaderConfig](envFile))
			Expect(err).NotTo(HaveOccurred())
			defer reader.Close()

			files, err := reader.List(nil, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(files).NotTo(BeEmpty())

			type record struct {
				Key  []byte
				Kind slatedb.RowEntryKind
				Val  []byte
			}
			var entries []record
			for _, f := range files {
				iter, err := f.Iterator()
				Expect(err).NotTo(HaveOccurred())

				for {
					entry, err := iter.Next()
					if err == io.EOF {
						break
					}
					Expect(err).NotTo(HaveOccurred())
					entries = append(entries, record{entry.Key, entry.Kind, entry.Value})
				}

				Expect(iter.Close()).NotTo(HaveOccurred())
				Expect(f.Close()).NotTo(HaveOccurred())
			}

			findEntry := func(key []byte) (record, bool) {
				for _, e := range entries {
					if string(e.Key) == string(key) {
						return e, true
					}
				}
				return record{}, false
			}

			valEntry, ok := findEntry([]byte("k_val"))
			Expect(ok).To(BeTrue())
			Expect(valEntry.Kind).To(Equal(slatedb.RowEntryKindValue))
			Expect(valEntry.Val).To(Equal([]byte("hello")))

			tombEntry, ok := findEntry([]byte("k_tomb"))
			Expect(ok).To(BeTrue())
			Expect(tombEntry.Kind).To(Equal(slatedb.RowEntryKindTombstone))
			Expect(tombEntry.Val).To(BeNil())
		})
	})

	Describe("List with range bounds", func() {
		It("should return empty for a start ID beyond all files", func() {
			seedDB(tmpDir, envFile)

			reader, err := slatedb.NewWalReader(tmpDir, slatedb.WithEnvFile[slatedb.WalReaderConfig](envFile))
			Expect(err).NotTo(HaveOccurred())
			defer reader.Close()

			veryHighID := uint64(999999)
			files, err := reader.List(&veryHighID, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(files).To(BeEmpty())
		})
	})
})
