package slatedb

/*
#include "slatedb.h"
*/
import "C"
import (
	"errors"
	"fmt"
)

// Error definitions.
var (
	ErrTransaction = errors.New("transaction error")
	ErrClosed      = errors.New("closed")
	ErrUnavailable = errors.New("unavailable")
	ErrInvalid     = errors.New("invalid")
	ErrData        = errors.New("data error")
	ErrInternal    = errors.New("internal error")
)

// CloseReason represents the reason that a DB instance was closed.
type CloseReason uint8

const (
	CloseReasonNone    CloseReason = 0
	CloseReasonClean   CloseReason = 1
	CloseReasonFenced  CloseReason = 2
	CloseReasonPanic   CloseReason = 3
	CloseReasonUnknown CloseReason = 255
)

func (reason CloseReason) String() string {
	switch reason {
	case CloseReasonNone:
		return "none"
	case CloseReasonClean:
		return "clean"
	case CloseReasonFenced:
		return "fenced"
	case CloseReasonPanic:
		return "panic"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(reason))
	}
}

// ClosedError provides structured close-reason details for closed DB errors.
type ClosedError struct {
	Reason CloseReason
	Msg    string
}

func (e *ClosedError) Error() string {
	if e == nil {
		return ErrClosed.Error()
	}
	if e.Msg == "" {
		if e.Reason == CloseReasonNone {
			return ErrClosed.Error()
		}
		return fmt.Sprintf("%s (reason=%s)", ErrClosed, e.Reason)
	}
	if e.Reason == CloseReasonNone {
		return fmt.Sprintf("%s: %s", ErrClosed, e.Msg)
	}
	return fmt.Sprintf("%s (reason=%s): %s", ErrClosed, e.Reason, e.Msg)
}

func (e *ClosedError) Unwrap() error {
	return ErrClosed
}

func normalizeCloseReason(reason CloseReason) CloseReason {
	switch reason {
	case CloseReasonNone, CloseReasonClean, CloseReasonFenced, CloseReasonPanic, CloseReasonUnknown:
		return reason
	default:
		return CloseReasonUnknown
	}
}

func resultToError(result C.struct_slatedb_result_t) error {
	if result.kind == C.SLATEDB_ERROR_KIND_NONE {
		return nil
	}

	if result.kind == C.SLATEDB_ERROR_KIND_CLOSED {
		reason := normalizeCloseReason(CloseReason(uint8(result.close_reason)))
		msg := ""
		if result.message != nil {
			msg = C.GoString(result.message)
		}
		return &ClosedError{
			Reason: reason,
			Msg:    msg,
		}
	}

	var baseErr error
	switch result.kind {
	case C.SLATEDB_ERROR_KIND_TRANSACTION:
		baseErr = ErrTransaction
	case C.SLATEDB_ERROR_KIND_UNAVAILABLE:
		baseErr = ErrUnavailable
	case C.SLATEDB_ERROR_KIND_INVALID:
		baseErr = ErrInvalid
	case C.SLATEDB_ERROR_KIND_DATA:
		baseErr = ErrData
	case C.SLATEDB_ERROR_KIND_INTERNAL:
		baseErr = ErrInternal
	default:
		baseErr = ErrInternal
	}

	if result.message != nil {
		return fmt.Errorf("%w: %s", baseErr, C.GoString(result.message))
	}
	return baseErr
}

func resultToErrorAndFree(result C.struct_slatedb_result_t) error {
	defer C.slatedb_result_free(result)
	return resultToError(result)
}
