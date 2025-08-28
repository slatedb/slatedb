package slatedb

/*
#cgo LDFLAGS: -lslatedb_go
#include "slatedb.h"
#include <stdlib.h>
*/
import "C"
import "unsafe"

// LogLevel represents the available logging levels
type LogLevel string

const (
	LogLevelTrace LogLevel = "trace"
	LogLevelDebug LogLevel = "debug"
	LogLevelInfo  LogLevel = "info"
	LogLevelWarn  LogLevel = "warn"
	LogLevelError LogLevel = "error"
)

// InitLogging initializes the Rust-side logging for SlateDB
// This should be called once before using any other SlateDB functions
// to enable detailed error logging that will help diagnose issues.
//
// Parameters:
//   - level: The minimum log level to display (trace, debug, info, warn, error)
//     Use LogLevelInfo for normal operation, LogLevelDebug or LogLevelTrace for debugging
//
// Example:
//
//	// Enable info-level logging (recommended for production)
//	err := slatedb.InitLogging(slatedb.LogLevelInfo)
//	if err != nil {
//	    log.Fatal("Failed to initialize logging:", err)
//	}
//
//	// Enable debug-level logging (for troubleshooting)
//	err := slatedb.InitLogging(slatedb.LogLevelDebug)
//	if err != nil {
//	    log.Fatal("Failed to initialize logging:", err)
//	}
func InitLogging(level LogLevel) error {
	cLevel := C.CString(string(level))
	defer C.free(unsafe.Pointer(cLevel))

	result := C.slatedb_init_logging(cLevel)
	defer C.slatedb_free_result(result)

	if result.error != C.Success {
		return resultToError(result)
	}

	return nil
}

// InitDefaultLogging initializes logging with the default info level
// This is a convenience function equivalent to InitLogging(LogLevelInfo)
func InitDefaultLogging() error {
	return InitLogging(LogLevelInfo)
}
