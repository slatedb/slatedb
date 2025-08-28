package slatedb_test

import (
	"io"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"slatedb.io/slatedb-go"
)

var _ = Describe("DB", func() {
	var (
		db     *slatedb.DB
		tmpDir string
	)

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "slatedb_db_test_*")
		Expect(err).NotTo(HaveOccurred())

		db, err = slatedb.Open(tmpDir, &slatedb.StoreConfig{
			Provider: slatedb.ProviderLocal,
		}, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(db).NotTo(BeNil())
	})

	AfterEach(func() {
		if db != nil {
			err := db.Close()
			Expect(err).NotTo(HaveOccurred())
		}
		os.RemoveAll(tmpDir)
	})

	Describe("Core Operations", func() {
		It("should put and get a key-value pair", func() {
			key := []byte("test_key")
			value := []byte("test_value")

			err := db.Put(key, value)
			Expect(err).NotTo(HaveOccurred())

			retrievedValue, err := db.Get(key)
			Expect(err).NotTo(HaveOccurred())
			Expect(retrievedValue).To(Equal(value))
		})

		It("should return ErrNotFound for non-existent key", func() {
			_, err := db.Get([]byte("non_existent"))
			Expect(err).To(Equal(slatedb.ErrNotFound))
		})

		It("should delete a key successfully", func() {
			key := []byte("delete_test")
			value := []byte("delete_value")

			err := db.Put(key, value)
			Expect(err).NotTo(HaveOccurred())

			err = db.Delete(key)
			Expect(err).NotTo(HaveOccurred())

			_, err = db.Get(key)
			Expect(err).To(Equal(slatedb.ErrNotFound))
		})
	})

	Describe("Operations with Options", func() {
		It("should put with custom options", func() {
			key := []byte("options_test")
			value := []byte("options_value")

			putOpts := &slatedb.PutOptions{TTLType: slatedb.TTLDefault}
			writeOpts := &slatedb.WriteOptions{AwaitDurable: true}

			err := db.PutWithOptions(key, value, putOpts, writeOpts)
			Expect(err).NotTo(HaveOccurred())

			retrievedValue, err := db.Get(key)
			Expect(err).NotTo(HaveOccurred())
			Expect(retrievedValue).To(Equal(value))
		})

		It("should get with custom read options", func() {
			key := []byte("read_options_test")
			value := []byte("read_options_value")

			err := db.Put(key, value)
			Expect(err).NotTo(HaveOccurred())

			readOpts := &slatedb.ReadOptions{
				DurabilityFilter: slatedb.DurabilityRemote,
				Dirty:            false,
			}

			retrievedValue, err := db.GetWithOptions(key, readOpts)
			Expect(err).NotTo(HaveOccurred())
			Expect(retrievedValue).To(Equal(value))
		})

		It("should delete with custom options", func() {
			key := []byte("delete_options_test")
			value := []byte("delete_options_value")

			err := db.Put(key, value)
			Expect(err).NotTo(HaveOccurred())

			writeOpts := &slatedb.WriteOptions{AwaitDurable: false}
			err = db.DeleteWithOptions(key, writeOpts)
			Expect(err).NotTo(HaveOccurred())

			_, err = db.Get(key)
			Expect(err).To(Equal(slatedb.ErrNotFound))
		})
	})

	Describe("Scan Operations", func() {
		BeforeEach(func() {
			testData := []slatedb.KeyValue{
				{Key: []byte("item:01"), Value: []byte("first")},
				{Key: []byte("item:02"), Value: []byte("second")},
				{Key: []byte("item:03"), Value: []byte("third")},
				{Key: []byte("other:1"), Value: []byte("other")},
			}

			// Use individual Put calls to populate test data
			for _, item := range testData {
				err := db.Put(item.Key, item.Value)
				Expect(err).NotTo(HaveOccurred())
			}
		})

		It("should create iterator for full scan", func() {
			iter, err := db.Scan(nil, nil)
			Expect(err).NotTo(HaveOccurred())
			defer iter.Close()

			count := 0
			for {
				_, err := iter.Next()
				if err == io.EOF {
					break
				}
				Expect(err).NotTo(HaveOccurred())
				count++
			}
			Expect(count).To(Equal(4))
		})

		It("should create iterator for range scan", func() {
			iter, err := db.Scan([]byte("item:"), []byte("item:99"))
			Expect(err).NotTo(HaveOccurred())
			defer iter.Close()

			count := 0
			for {
				kv, err := iter.Next()
				if err == io.EOF {
					break
				}
				Expect(err).NotTo(HaveOccurred())
				Expect(string(kv.Key)).To(HavePrefix("item:"))
				count++
			}
			Expect(count).To(Equal(3))
		})

		It("should scan with custom options", func() {
			opts := &slatedb.ScanOptions{
				DurabilityFilter: slatedb.DurabilityRemote,
				Dirty:            false,
				ReadAheadBytes:   1024,
				CacheBlocks:      true,
			}

			iter, err := db.ScanWithOptions([]byte("item:"), []byte("item:99"), opts)
			Expect(err).NotTo(HaveOccurred())
			defer iter.Close()

			count := 0
			for {
				kv, err := iter.Next()
				if err == io.EOF {
					break
				}
				Expect(err).NotTo(HaveOccurred())
				Expect(string(kv.Key)).To(HavePrefix("item:"))
				count++
			}
			Expect(count).To(Equal(3))
		})
	})

	Describe("Database Management", func() {
		It("should open and close database successfully", func() {
			err := db.Put([]byte("lifecycle_test"), []byte("test"))
			Expect(err).NotTo(HaveOccurred())

			err = db.Close()
			Expect(err).NotTo(HaveOccurred())

			db = nil // Prevent double close in AfterEach
		})
	})
})
