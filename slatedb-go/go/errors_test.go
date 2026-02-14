package slatedb_test

import (
	stderrors "errors"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"slatedb.io/slatedb-go"
)

var _ = Describe("Errors", func() {
	Describe("CloseReason", func() {
		It("should expose stable string values", func() {
			Expect(slatedb.CloseReasonNone.String()).To(Equal("none"))
			Expect(slatedb.CloseReasonClean.String()).To(Equal("clean"))
			Expect(slatedb.CloseReasonFenced.String()).To(Equal("fenced"))
			Expect(slatedb.CloseReasonPanic.String()).To(Equal("panic"))
			Expect(slatedb.CloseReasonUnknown.String()).To(Equal("unknown(255)"))
			Expect(slatedb.CloseReason(42).String()).To(Equal("unknown(42)"))
		})
	})

	Describe("ClosedError", func() {
		It("should format with reason and message", func() {
			err := &slatedb.ClosedError{
				Reason: slatedb.CloseReasonFenced,
				Msg:    "detected newer DB client",
			}
			Expect(err.Error()).To(Equal("closed (reason=fenced): detected newer DB client"))
		})

		It("should format without message", func() {
			err := &slatedb.ClosedError{
				Reason: slatedb.CloseReasonClean,
			}
			Expect(err.Error()).To(Equal("closed (reason=clean)"))
		})

		It("should behave as ErrClosed with errors.Is", func() {
			err := &slatedb.ClosedError{
				Reason: slatedb.CloseReasonPanic,
				Msg:    "background task panicked",
			}
			wrapped := fmt.Errorf("operation failed: %w", err)

			Expect(stderrors.Is(err, slatedb.ErrClosed)).To(BeTrue())
			Expect(stderrors.Is(wrapped, slatedb.ErrClosed)).To(BeTrue())
			Expect(stderrors.Is(wrapped, slatedb.ErrInvalid)).To(BeFalse())
		})

		It("should support extraction with errors.As", func() {
			expected := &slatedb.ClosedError{
				Reason: slatedb.CloseReasonPanic,
				Msg:    "background task panicked",
			}
			wrapped := fmt.Errorf("outer: %w", expected)

			var actual *slatedb.ClosedError
			Expect(stderrors.As(wrapped, &actual)).To(BeTrue())
			Expect(actual).NotTo(BeNil())
			Expect(actual.Reason).To(Equal(slatedb.CloseReasonPanic))
			Expect(actual.Msg).To(Equal("background task panicked"))
		})
	})
})
