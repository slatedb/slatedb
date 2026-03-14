package ffi

/*
#cgo CFLAGS: -I${SRCDIR}
#cgo darwin LDFLAGS: -L${SRCDIR}/../../target/debug -L${SRCDIR}/../../target/release -lslatedb_ffi
#cgo linux LDFLAGS: -L${SRCDIR}/../../target/debug -L${SRCDIR}/../../target/release -lslatedb_ffi
#cgo windows LDFLAGS: -L${SRCDIR}/../../target/debug -L${SRCDIR}/../../target/release -lslatedb_ffi
*/
import "C"
