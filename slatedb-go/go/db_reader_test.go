package slatedb_test

import (
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"slatedb.io/slatedb-go"
)

var _ = Describe("DbReader", func() {
	var (
		config   = &slatedb.StoreConfig{Provider: slatedb.ProviderLocal}
		db       *slatedb.DB
		dbReader *slatedb.DbReader
		tmpDir   string
	)

	BeforeAll(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "slatedb_db_test_*")
		Expect(err).NotTo(HaveOccurred())

		db, err := slatedb.Open(tmpDir, config)
		Expect(err).NotTo(HaveOccurred())
		Expect(db).NotTo(BeNil())

		Expect(db.Put([]byte("test_key"), []byte("test_value"))).NotTo(HaveOccurred())
		Expect(db.Flush()).NotTo(HaveOccurred())
	})

	BeforeEach(func() {
		dbReader, err := slatedb.OpenReader(tmpDir, config, nil, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(dbReader).NotTo(BeNil())
	})

	AfterEach(func() {
		if dbReader != nil {
			err := dbReader.Close()
			Expect(err).NotTo(HaveOccurred())
		}
	})

	AfterAll(func() {
		if db != nil {
			Expect(db.Close()).NotTo(HaveOccurred())
		}
		Expect(os.RemoveAll(tmpDir)).NotTo(HaveOccurred())
	})

	Describe("Core Operations", func() {
		It("should return ErrNotFound for non-existent key", func() {
			_, err := dbReader.Get([]byte("non_existent"))
			Expect(err).To(Equal(slatedb.ErrNotFound))
		})
	})
})
