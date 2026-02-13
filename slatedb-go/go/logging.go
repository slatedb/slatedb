package slatedb

/*
#include "slatedb.h"
#include <stdlib.h>
*/
import "C"
import (
	"fmt"
	"strings"
)

// LogLevel represents the available logging levels.
type LogLevel string

const (
	LogLevelTrace LogLevel = "trace"
	LogLevelDebug LogLevel = "debug"
	LogLevelInfo  LogLevel = "info"
	LogLevelWarn  LogLevel = "warn"
	LogLevelError LogLevel = "error"
)

func logLevelToC(level LogLevel) (C.slatedb_log_level_t, error) {
	switch strings.ToLower(string(level)) {
	case "off":
		return C.slatedb_log_level_t(C.SLATEDB_LOG_LEVEL_OFF), nil
	case "error":
		return C.slatedb_log_level_t(C.SLATEDB_LOG_LEVEL_ERROR), nil
	case "warn", "warning":
		return C.slatedb_log_level_t(C.SLATEDB_LOG_LEVEL_WARN), nil
	case "info", "":
		return C.slatedb_log_level_t(C.SLATEDB_LOG_LEVEL_INFO), nil
	case "debug":
		return C.slatedb_log_level_t(C.SLATEDB_LOG_LEVEL_DEBUG), nil
	case "trace":
		return C.slatedb_log_level_t(C.SLATEDB_LOG_LEVEL_TRACE), nil
	default:
		return 0, fmt.Errorf("invalid log level: %s", level)
	}
}

// InitLogging initializes the Rust-side logging for SlateDB.
func InitLogging(level LogLevel) error {
	cLevel, err := logLevelToC(level)
	if err != nil {
		return err
	}

	result := C.slatedb_logging_init(cLevel)
	return resultToErrorAndFree(result)
}

// SetLoggingLevel updates the global logging level for slatedb-c logger output.
func SetLoggingLevel(level LogLevel) error {
	cLevel, err := logLevelToC(level)
	if err != nil {
		return err
	}

	result := C.slatedb_logging_set_level(cLevel)
	return resultToErrorAndFree(result)
}

// InitDefaultLogging initializes logging with the default info level.
func InitDefaultLogging() error {
	return InitLogging(LogLevelInfo)
}
