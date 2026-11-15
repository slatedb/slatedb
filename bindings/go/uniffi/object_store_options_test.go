package slatedb_test

import (
	"errors"
	"testing"

	slatedb "slatedb.io/slatedb-go/uniffi"
)

func TestObjectStoreResolveWithOptionsAcceptsS3Credentials(t *testing.T) {
	store, err := slatedb.ObjectStoreResolveWithOptions("s3://my-bucket", map[string]string{
		"aws_access_key_id":     "AKIDEXAMPLE",
		"aws_secret_access_key": "secret",
		"aws_session_token":     "token",
		"aws_region":            "us-east-1",
	})
	if err != nil {
		t.Fatalf("ObjectStoreResolveWithOptions(s3://my-bucket): %v", err)
	}
	store.Destroy()
}

func TestObjectStoreResolveWithOptionsRejectsUnknownS3Key(t *testing.T) {
	store, err := slatedb.ObjectStoreResolveWithOptions("s3://my-bucket", map[string]string{
		"aws_acess_key_id": "AKIDEXAMPLE",
	})
	if err == nil {
		store.Destroy()
	}
	if !errors.Is(err, slatedb.ErrErrorInvalid) {
		t.Fatalf("unknown key gave %v, want an invalid-argument error", err)
	}
}
