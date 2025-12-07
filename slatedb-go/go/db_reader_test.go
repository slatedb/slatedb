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
		envFile  string
	)

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "slatedb_db_test_*")
		Expect(err).NotTo(HaveOccurred())

		envFile, err = createEnvFile(tmpDir)
		Expect(err).NotTo(HaveOccurred())

		db, err = slatedb.Open(tmpDir, slatedb.WithEnvFile[slatedb.DbConfig](envFile))
		Expect(err).NotTo(HaveOccurred())
		Expect(db).NotTo(BeNil())

		Expect(db.Put([]byte("test_key"), []byte("test_value"))).NotTo(HaveOccurred())
		Expect(db.Flush()).NotTo(HaveOccurred())

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

		It("should return ErrNotFound for non-existent key", func() {
			_, err := dbReader.Get([]byte("non_existent"))
			Expect(err).To(Equal(slatedb.ErrNotFound))
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
		It("should scan prefix including trailing 0xff", func() {
			Expect(db.Put([]byte("pref\xff"), []byte("v1"))).NotTo(HaveOccurred())
			Expect(db.Put([]byte("pref\xff\x00"), []byte("v2"))).NotTo(HaveOccurred())
			Expect(db.Put([]byte("pref\x01"), []byte("skip"))).NotTo(HaveOccurred())
			Expect(db.Flush()).NotTo(HaveOccurred())

			Expect(dbReader.Close()).NotTo(HaveOccurred())
			var err error
			dbReader, err = slatedb.OpenReader(tmpDir, slatedb.WithEnvFile[slatedb.DbReaderConfig](envFile))
			Expect(err).NotTo(HaveOccurred())

			iter, err := dbReader.ScanPrefix([]byte("pref\xff"))
			Expect(err).NotTo(HaveOccurred())
			defer func() { Expect(iter.Close()).NotTo(HaveOccurred()) }()

			var keys []string
			for {
				kv, err := iter.Next()
				if err == io.EOF {
					break
				}
				Expect(err).NotTo(HaveOccurred())
				keys = append(keys, string(kv.Key))
			}
			Expect(keys).To(Equal([]string{"pref\xff", "pref\xff\x00"}))
		})
	})
})
