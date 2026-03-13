package slatedb

// #cgo darwin LDFLAGS: -L${SRCDIR}/../../target/debug -L${SRCDIR}/../../target/release -lslatedb_ffi -Wl,-rpath,${SRCDIR}/../../target/debug -Wl,-rpath,${SRCDIR}/../../target/release
// #cgo linux LDFLAGS: -L${SRCDIR}/../../target/debug -L${SRCDIR}/../../target/release -lslatedb_ffi -Wl,-rpath,${SRCDIR}/../../target/debug -Wl,-rpath,${SRCDIR}/../../target/release
import "C"
