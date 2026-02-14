package io.slatedb;

import java.util.Objects;

/// Base runtime exception thrown when SlateDB returns an error.
public class SlateDbException extends RuntimeException {
    protected SlateDbException(String message) {
        super(message);
    }

    /// Transaction conflict or transactional error.
    public static final class TransactionException extends SlateDbException {
        TransactionException(String message) {
            super(message);
        }
    }

    /// Database is closed and no longer usable.
    public static final class ClosedException extends SlateDbException {
        private final String closeReason;
        private final int closeReasonCode;

        ClosedException(String closeReason, int closeReasonCode, String message) {
            super(message);
            this.closeReason = Objects.requireNonNull(closeReason, "closeReason");
            this.closeReasonCode = closeReasonCode;
        }

        /// Returns the close reason for this closed error.
        public String getCloseReason() {
            return closeReason;
        }

        /// Returns the native close-reason code.
        public int getCloseReasonCode() {
            return closeReasonCode;
        }
    }

    /// Backend or storage service unavailable.
    public static final class UnavailableException extends SlateDbException {
        UnavailableException(String message) {
            super(message);
        }
    }

    /// Invalid input or invalid operation requested by caller.
    public static final class InvalidException extends SlateDbException {
        InvalidException(String message) {
            super(message);
        }
    }

    /// Data corruption or incompatible persisted data state.
    public static final class DataException extends SlateDbException {
        DataException(String message) {
            super(message);
        }
    }

    /// Internal SlateDB error.
    public static final class InternalException extends SlateDbException {
        InternalException(String message) {
            super(message);
        }
    }

    /// Forward-compatible fallback for unknown native error kinds.
    public static final class UnknownException extends SlateDbException {
        UnknownException(String message) {
            super(message);
        }
    }
}
