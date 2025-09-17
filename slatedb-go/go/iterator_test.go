package slatedb_test

import (
	"io"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"slatedb.io/slatedb-go"
)

var _ = Describe("Iterator", func() {
	var (
		db     *slatedb.DB
		tmpDir string
		iter   *slatedb.Iterator
	)

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "slatedb_iterator_test_*")
		Expect(err).NotTo(HaveOccurred())

		db, err = slatedb.Open(tmpDir, &slatedb.StoreConfig{
			Provider: slatedb.ProviderLocal,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(db).NotTo(BeNil())

		// Populate test data
		testData := []slatedb.KeyValue{
			{Key: []byte("item:01"), Value: []byte("first")},
			{Key: []byte("item:02"), Value: []byte("second")},
			{Key: []byte("item:03"), Value: []byte("third")},
			{Key: []byte("other:1"), Value: []byte("other")},
		}

		for _, item := range testData {
			err := db.Put(item.Key, item.Value)
			Expect(err).NotTo(HaveOccurred())
		}

		// Create iterator for tests
		iter, err = db.Scan(nil, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(iter).NotTo(BeNil())
	})

	AfterEach(func() {
		if iter != nil {
			iter.Close()
		}
		if db != nil {
			err := db.Close()
			Expect(err).NotTo(HaveOccurred())
		}
		os.RemoveAll(tmpDir)
	})

	Describe("Iterator Methods", func() {
		It("should iterate through all items with Next()", func() {
			count := 0
			var lastKey []byte

			for {
				kv, err := iter.Next()
				if err == io.EOF {
					break
				}
				Expect(err).NotTo(HaveOccurred())
				Expect(len(kv.Key)).To(BeNumerically(">", 0))
				Expect(len(kv.Value)).To(BeNumerically(">", 0))

				// Keys should be in order
				if lastKey != nil {
					Expect(string(kv.Key) >= string(lastKey)).To(BeTrue(),
						"Keys should be in lexicographical order: %s >= %s", string(kv.Key), string(lastKey))
				}
				lastKey = kv.Key
				count++
			}
			Expect(count).To(Equal(4))
		})

		It("should seek to a specific key position", func() {
			err := iter.Seek([]byte("item:02"))
			Expect(err).NotTo(HaveOccurred())

			kv, err := iter.Next()
			Expect(err).NotTo(HaveOccurred())
			Expect(string(kv.Key) >= "item:02").To(BeTrue(),
				"Key should be >= 'item:02', got: %s", string(kv.Key))
		})

		It("should seek beyond all keys", func() {
			err := iter.Seek([]byte("zzz"))
			Expect(err).NotTo(HaveOccurred())

			// Next should return EOF since we seeked beyond all keys
			_, err = iter.Next()
			Expect(err).To(Equal(io.EOF))
		})

		It("should handle multiple seeks", func() {
			// Seek to beginning
			err := iter.Seek([]byte("item:01"))
			Expect(err).NotTo(HaveOccurred())

			kv, err := iter.Next()
			Expect(err).NotTo(HaveOccurred())
			Expect(string(kv.Key) >= "item:01").To(BeTrue(),
				"Key should be >= 'item:01', got: %s", string(kv.Key))

			// Seek forward
			err = iter.Seek([]byte("item:03"))
			Expect(err).NotTo(HaveOccurred())

			kv, err = iter.Next()
			Expect(err).NotTo(HaveOccurred())
			Expect(string(kv.Key) >= "item:03").To(BeTrue(),
				"Key should be >= 'item:03', got: %s", string(kv.Key))
		})

		It("should handle close properly", func() {
			err := iter.Close()
			Expect(err).NotTo(HaveOccurred())

			// Trying to use iterator after close should fail
			_, err = iter.Next()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("iterator is closed"))

			err = iter.Seek([]byte("test"))
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("iterator is closed"))

			// Multiple close calls should be safe
			err = iter.Close()
			Expect(err).NotTo(HaveOccurred())

			iter = nil // Prevent double close in AfterEach
		})

		It("should handle seek with empty key", func() {
			err := iter.Seek([]byte{})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("seek key cannot be empty"))
		})

		It("should handle Next() after reaching end", func() {
			// Exhaust the iterator
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

			// Additional Next() calls should continue returning EOF
			_, err := iter.Next()
			Expect(err).To(Equal(io.EOF))

			_, err = iter.Next()
			Expect(err).To(Equal(io.EOF))
		})
	})
})
