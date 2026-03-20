package io.slatedb.uniffi;


import java.util.List;
import java.util.Map;

/**
 * Log level used by [`init_logging`].
 */

public enum LogLevel {
    /**
     * Disable logging.
     */
  OFF,
    /**
     * Error-level logs only.
     */
  ERROR,
    /**
     * Warning and error logs.
     */
  WARN,
    /**
     * Info, warning, and error logs.
     */
  INFO,
    /**
     * Debug, info, warning, and error logs.
     */
  DEBUG,
    /**
     * Trace and all higher-severity logs.
     */
  TRACE;
}


