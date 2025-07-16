#!/usr/bin/env bash

# Wrapper script for nextest to print a gdb backtrace when nextest cancels
# the test due to a timeout.

# Fail on errors.
set -eu

# Run background jobs in separate process groups, so that the SIGTERM from
# nextest doesn't hit the test job until we're ready.
set -m

dump_backtrace() {
    sudo gdb -p "$pid" -batch -ex "thread apply all bt" 2>/dev/null

    # Hard kill the test process, if it's still running. We don't bother sending
    # a graceful SIGTERM to the test process because none of our tests need to
    # do any cleanup. If that changes, this can be revisited.
    kill -KILL "$pid" || true

    # Remove our SIGTERM handler and re-raise SIGTERM so that our exit status
    # correctly reflects that we exited in response to a SIGTERM.
    trap - TERM
    kill -TERM "$$"
}

# Execute the test in the background and capture its PID.
"$@" &
pid=$!

# Arrange to dump a backtrace when nextest sends a SIGTERM.
trap dump_backtrace TERM

# Wait for the test to finish. `set -e` ensures the script exits with a failing
# exit code if the test exits with a failing exit code.
wait "$pid"
