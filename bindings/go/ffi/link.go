package ffi

/*
#cgo CFLAGS: -I${SRCDIR}
#cgo darwin LDFLAGS: -L${SRCDIR}/../../../target/debug -L${SRCDIR}/../../../target/release -lslatedb_uniffi
#cgo linux LDFLAGS: -L${SRCDIR}/../../../target/debug -L${SRCDIR}/../../../target/release -lslatedb_uniffi
#cgo windows LDFLAGS: -L${SRCDIR}/../../../target/debug -L${SRCDIR}/../../../target/release -lslatedb_uniffi
*/
import "C"
