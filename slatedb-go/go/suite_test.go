package slatedb_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSlateDB(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "SlateDB Test Suite")
}
