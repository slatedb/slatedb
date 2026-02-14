package slatedb_test

import (
	"io"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"slatedb.io/slatedb-go"
)

var _ = Describe("DbReader", func() {
	var (
		db       *slatedb.DB
		dbReader *slatedb.DbReader
		tmpDir   string
	)

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "slatedb_db_test_*")
		Expect(err).NotTo(HaveOccurred())

		envFile, err := createEnvFile(tmpDir)
		Expect(err).NotTo(HaveOccurred())

		db, err = slatedb.Open(tmpDir, slatedb.WithEnvFile[slatedb.DbConfig](envFile))
		Expect(err).NotTo(HaveOccurred())
		Expect(db).NotTo(BeNil())

		testData := []slatedb.KeyValue{
			{Key: []byte("test_key"), Value: []byte("test_value")},
			{Key: []byte("item:01"), Value: []byte("first")},
			{Key: []byte("item:02"), Value: []byte("second")},
			{Key: []byte("item:03"), Value: []byte("third")},
			{Key: []byte("other:1"), Value: []byte("other")},
		}

		for _, item := range testData {
			_, err := db.Put(item.Key, item.Value)
			Expect(err).NotTo(HaveOccurred())
		}
		Expect(db.Flush()).NotTo(HaveOccurred())
		Expect(db.Close()).NotTo(HaveOccurred())
		db = nil

		dbReader, err = slatedb.OpenReader(tmpDir, slatedb.WithEnvFile[slatedb.DbReaderConfig](envFile))
		Expect(err).NotTo(HaveOccurred())
		Expect(dbReader).NotTo(BeNil())
	})

	AfterEach(func() {
		if dbReader != nil {
			Expect(dbReader.Close()).NotTo(HaveOccurred())
		}
		if db != nil {
			Expect(db.Close()).NotTo(HaveOccurred())
		}
		Expect(os.RemoveAll(tmpDir)).NotTo(HaveOccurred())
	})

	Describe("Core Operations", func() {
		It("should get a key-value pair", func() {
			key := []byte("test_key")
			value := []byte("test_value")

			retrievedValue, err := dbReader.Get(key)
			Expect(err).NotTo(HaveOccurred())
			Expect(retrievedValue).To(Equal(value))
		})

		It("should return nil,nil for non-existent key", func() {
			value, err := dbReader.Get([]byte("non_existent"))
			Expect(err).NotTo(HaveOccurred())
			Expect(value).To(BeNil())
		})

		It("should distinguish empty value from missing key", func() {
			if dbReader != nil {
				Expect(dbReader.Close()).NotTo(HaveOccurred())
				dbReader = nil
			}

			envFile, err := createEnvFile(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			db, err = slatedb.Open(tmpDir, slatedb.WithEnvFile[slatedb.DbConfig](envFile))
			Expect(err).NotTo(HaveOccurred())
			Expect(db.Put([]byte("empty_value_key"), []byte{})).NotTo(HaveOccurred())
			Expect(db.Flush()).NotTo(HaveOccurred())
			Expect(db.Close()).NotTo(HaveOccurred())
			db = nil

			dbReader, err = slatedb.OpenReader(tmpDir, slatedb.WithEnvFile[slatedb.DbReaderConfig](envFile))
			Expect(err).NotTo(HaveOccurred())

			value, err := dbReader.Get([]byte("empty_value_key"))
			Expect(err).NotTo(HaveOccurred())
			Expect(value).To(Equal([]byte{}))
			Expect(value).NotTo(BeNil())

			missing, err := dbReader.Get([]byte("non_existent_after_reopen"))
			Expect(err).NotTo(HaveOccurred())
			Expect(missing).To(BeNil())
		})

		It("should return error if database does not exist", func() {
			newTmpDir, err := os.MkdirTemp("", "slatedb_db_test_*")
			Expect(err).NotTo(HaveOccurred())
			defer func() { Expect(os.RemoveAll(newTmpDir)).NotTo(HaveOccurred()) }()

			envFile, err := createEnvFile(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			_, err = slatedb.OpenReader(newTmpDir, slatedb.WithEnvFile[slatedb.DbReaderConfig](envFile))
			Expect(err).To(MatchError(slatedb.ErrData))
		})
	})

	Describe("Operations with Options", func() {
		DescribeTable(
			"should get with custom read options",
			func(opts *slatedb.ReadOptions) {
				key := []byte("test_key")
				value := []byte("test_value")
				retrievedValue, err := dbReader.GetWithOptions(key, opts)
				Expect(err).NotTo(HaveOccurred())
				Expect(retrievedValue).To(Equal(value))
			},
			Entry("memory not dirty", &slatedb.ReadOptions{DurabilityFilter: slatedb.DurabilityMemory}),
			Entry("memory dirty", &slatedb.ReadOptions{DurabilityFilter: slatedb.DurabilityMemory, Dirty: true}),
			Entry("remote not dirty", &slatedb.ReadOptions{DurabilityFilter: slatedb.DurabilityRemote}),
			Entry("remote dirty", &slatedb.ReadOptions{DurabilityFilter: slatedb.DurabilityRemote, Dirty: true}),
		)
	})

	Describe("Scan Operations", func() {
		It("should create iterator for full scan", func() {
			iter, err := dbReader.Scan(nil, nil)
			Expect(err).NotTo(HaveOccurred())
			defer func() { Expect(iter.Close()).NotTo(HaveOccurred()) }()

			count := 0
			for {
				_, err := iter.Next()
				if err == io.EOF {
					break
				}
				Expect(err).NotTo(HaveOccurred())
				count++
			}
			Expect(count).To(Equal(5))
		})

		It("should create iterator for range scan", func() {
			iter, err := dbReader.Scan([]byte("item:"), []byte("item:99"))
			Expect(err).NotTo(HaveOccurred())
			defer func() { Expect(iter.Close()).NotTo(HaveOccurred()) }()

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

			iter, err := dbReader.ScanWithOptions([]byte("item:"), []byte("item:99"), opts)
			Expect(err).NotTo(HaveOccurred())
			defer func() { Expect(iter.Close()).NotTo(HaveOccurred()) }()

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

		It("should scan by prefix", func() {
			iter, err := dbReader.ScanPrefix([]byte("item:"))
			Expect(err).NotTo(HaveOccurred())
			defer func() { Expect(iter.Close()).NotTo(HaveOccurred()) }()

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

		It("should scan by prefix with custom options", func() {
			opts := &slatedb.ScanOptions{
				DurabilityFilter: slatedb.DurabilityRemote,
				Dirty:            false,
				ReadAheadBytes:   1024,
				CacheBlocks:      true,
				MaxFetchTasks:    2,
			}

			iter, err := dbReader.ScanPrefixWithOptions([]byte("item:"), opts)
			Expect(err).NotTo(HaveOccurred())
			defer func() { Expect(iter.Close()).NotTo(HaveOccurred()) }()

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
})
