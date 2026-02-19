package slatedb_test

import (
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"slatedb.io/slatedb-go"
)

var _ = Describe("WriteBatch", func() {
	var (
		db     *slatedb.DB
		tmpDir string
	)

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "slatedb_batch_test_*")
		Expect(err).NotTo(HaveOccurred())

		envFile, err := createEnvFile(tmpDir)
		Expect(err).NotTo(HaveOccurred())

		db, err = slatedb.Open(tmpDir, slatedb.WithEnvFile[slatedb.DbConfig](envFile))
		Expect(err).NotTo(HaveOccurred())
		Expect(db).NotTo(BeNil())
	})

	AfterEach(func() {
		if db != nil {
			err := db.Close()
			Expect(err).NotTo(HaveOccurred())
		}
		Expect(os.RemoveAll(tmpDir)).NotTo(HaveOccurred())
	})

	Describe("Batch Operations", func() {
		It("should create and execute a simple batch", func() {
			batch, err := slatedb.NewWriteBatch()
			Expect(err).NotTo(HaveOccurred())
			Expect(batch).NotTo(BeNil())
			defer batch.Close()

			err = batch.Put([]byte("batch_key1"), []byte("batch_value1"))
			Expect(err).NotTo(HaveOccurred())

			err = batch.Put([]byte("batch_key2"), []byte("batch_value2"))
			Expect(err).NotTo(HaveOccurred())

			_, err = db.Write(batch)
			Expect(err).NotTo(HaveOccurred())

			// Verify the writes
			value1, err := db.Get([]byte("batch_key1"))
			Expect(err).NotTo(HaveOccurred())
			Expect(value1).To(Equal([]byte("batch_value1")))

			value2, err := db.Get([]byte("batch_key2"))
			Expect(err).NotTo(HaveOccurred())
			Expect(value2).To(Equal([]byte("batch_value2")))
		})

		It("should handle mixed put and delete operations", func() {
			// Pre-populate some data
			_, err := db.Put([]byte("existing_key"), []byte("existing_value"))
			Expect(err).NotTo(HaveOccurred())

			batch, err := slatedb.NewWriteBatch()
			Expect(err).NotTo(HaveOccurred())
			defer batch.Close()

			err = batch.Put([]byte("new_key"), []byte("new_value"))
			Expect(err).NotTo(HaveOccurred())

			err = batch.Delete([]byte("existing_key"))
			Expect(err).NotTo(HaveOccurred())

			_, err = db.Write(batch)
			Expect(err).NotTo(HaveOccurred())

			// Verify new key exists
			value, err := db.Get([]byte("new_key"))
			Expect(err).NotTo(HaveOccurred())
			Expect(value).To(Equal([]byte("new_value")))

			// Verify existing key is deleted
			value, err = db.Get([]byte("existing_key"))
			Expect(err).NotTo(HaveOccurred())
			Expect(value).To(BeNil())
		})

		It("should support PutWithOptions", func() {
			batch, err := slatedb.NewWriteBatch()
			Expect(err).NotTo(HaveOccurred())
			defer batch.Close()

			putOpts := &slatedb.PutOptions{TTLType: slatedb.TTLDefault}
			err = batch.PutWithOptions([]byte("ttl_key"), []byte("ttl_value"), putOpts)
			Expect(err).NotTo(HaveOccurred())

			_, err = db.Write(batch)
			Expect(err).NotTo(HaveOccurred())

			// Verify the write
			value, err := db.Get([]byte("ttl_key"))
			Expect(err).NotTo(HaveOccurred())
			Expect(value).To(Equal([]byte("ttl_value")))
		})

		It("should prevent reuse after write", func() {
			batch, err := slatedb.NewWriteBatch()
			Expect(err).NotTo(HaveOccurred())
			defer batch.Close()

			err = batch.Put([]byte("key1"), []byte("value1"))
			Expect(err).NotTo(HaveOccurred())

			_, err = db.Write(batch)
			Expect(err).NotTo(HaveOccurred())

			// Trying to use batch after write should fail
			err = batch.Put([]byte("key2"), []byte("value2"))
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("already consumed"))

			// Trying to write again should fail
			_, err = db.Write(batch)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("already consumed"))
		})

		It("should prevent use after close", func() {
			batch, err := slatedb.NewWriteBatch()
			Expect(err).NotTo(HaveOccurred())

			err = batch.Put([]byte("key1"), []byte("value1"))
			Expect(err).NotTo(HaveOccurred())

			err = batch.Close()
			Expect(err).NotTo(HaveOccurred())

			// Trying to use batch after close should fail
			err = batch.Put([]byte("key2"), []byte("value2"))
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("batch is closed"))

			err = batch.Delete([]byte("key3"))
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("batch is closed"))

			_, err = db.Write(batch)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("batch is closed"))
		})

		It("should handle WriteWithOptions", func() {
			batch, err := slatedb.NewWriteBatch()
			Expect(err).NotTo(HaveOccurred())
			defer batch.Close()

			err = batch.Put([]byte("key1"), []byte("value1"))
			Expect(err).NotTo(HaveOccurred())

			writeOpts := &slatedb.WriteOptions{AwaitDurable: false}
			_, err = db.WriteWithOptions(batch, writeOpts)
			Expect(err).NotTo(HaveOccurred())

			// Verify the write
			value, err := db.Get([]byte("key1"))
			Expect(err).NotTo(HaveOccurred())
			Expect(value).To(Equal([]byte("value1")))
		})

		It("should validate empty keys", func() {
			batch, err := slatedb.NewWriteBatch()
			Expect(err).NotTo(HaveOccurred())
			defer batch.Close()

			// Empty key should fail for Put
			err = batch.Put([]byte{}, []byte("value"))
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("key cannot be empty"))

			// Empty key should fail for Delete
			err = batch.Delete([]byte{})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("key cannot be empty"))
		})

		It("should handle order consistency for duplicate keys", func() {
			batch, err := slatedb.NewWriteBatch()
			Expect(err).NotTo(HaveOccurred())
			defer batch.Close()

			// Add operations for the same key in order
			err = batch.Put([]byte("dup_key"), []byte("value1"))
			Expect(err).NotTo(HaveOccurred())

			err = batch.Put([]byte("dup_key"), []byte("value2"))
			Expect(err).NotTo(HaveOccurred())

			err = batch.Delete([]byte("dup_key"))
			Expect(err).NotTo(HaveOccurred())

			err = batch.Put([]byte("dup_key"), []byte("final_value"))
			Expect(err).NotTo(HaveOccurred())

			_, err = db.Write(batch)
			Expect(err).NotTo(HaveOccurred())

			// Should see the final put operation
			value, err := db.Get([]byte("dup_key"))
			Expect(err).NotTo(HaveOccurred())
			Expect(value).To(Equal([]byte("final_value")))
		})
	})

	Describe("WriteHandle", func() {
		It("should return valid WriteHandle for Write", func() {
			batch, err := slatedb.NewWriteBatch()
			Expect(err).NotTo(HaveOccurred())
			defer batch.Close()

			err = batch.Put([]byte("key"), []byte("value"))
			Expect(err).NotTo(HaveOccurred())

			wh, err := db.Write(batch)
			Expect(err).NotTo(HaveOccurred())
			Expect(wh).NotTo(BeNil())
			Expect(wh.Seq).To(Equal(uint64(1)))
			Expect(wh.CreateTs).NotTo(BeNil())
			Expect(*wh.CreateTs).To(BeNumerically(">", 0))
		})

		It("should return valid WriteHandle for WriteWithOptions", func() {
			batch, err := slatedb.NewWriteBatch()
			Expect(err).NotTo(HaveOccurred())
			defer batch.Close()

			err = batch.Put([]byte("key"), []byte("value"))
			Expect(err).NotTo(HaveOccurred())

			writeOpts := &slatedb.WriteOptions{AwaitDurable: true}
			wh, err := db.WriteWithOptions(batch, writeOpts)
			Expect(err).NotTo(HaveOccurred())
			Expect(wh).NotTo(BeNil())
			Expect(wh.Seq).To(Equal(uint64(1)))
			Expect(wh.CreateTs).NotTo(BeNil())
			Expect(*wh.CreateTs).To(BeNumerically(">", 0))
		})

		It("should return increasing Seq for subsequent writes", func() {
			// First write
			batch1, err := slatedb.NewWriteBatch()
			Expect(err).NotTo(HaveOccurred())
			defer batch1.Close()

			err = batch1.Put([]byte("key1"), []byte("value1"))
			Expect(err).NotTo(HaveOccurred())

			wh1, err := db.Write(batch1)
			Expect(err).NotTo(HaveOccurred())

			Expect(wh1.Seq).To(Equal(uint64(1)))
			Expect(wh1.CreateTs).NotTo(BeNil())
			Expect(*wh1.CreateTs).To(BeNumerically(">", 0))

			// Second write
			batch2, err := slatedb.NewWriteBatch()
			Expect(err).NotTo(HaveOccurred())
			defer batch2.Close()

			err = batch2.Put([]byte("key2"), []byte("value2"))
			Expect(err).NotTo(HaveOccurred())

			wh2, err := db.Write(batch2)
			Expect(err).NotTo(HaveOccurred())

			Expect(wh2.Seq).To(Equal(uint64(2)))
			Expect(wh2.CreateTs).NotTo(BeNil())
			Expect(*wh2.CreateTs).To(BeNumerically(">", 0))
		})
	})

})
