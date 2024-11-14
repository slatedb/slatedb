# C bindings for SlateDB

This codebase contains the Rust FFI code under the src directory that exposes the slatedb API to C. We use cbindgen to generate the C code. cbindgen is invoked by `build.rs` file which generates the C bindings in the `bindings/c/include/slatedb.h` file.

This header file can be used by C code to interface with slatedb. Take a look at the files under the `bindings/c/examples` to get a better understading of the API usage.

## Build and run the code
From the bindings/c directory, run `make build` to build the rust FFI code, generate the C header file, and compile the C example code.

After completing the build to run the example code for in-memory backed slated db run `make run-memory`, for S3 backed example code run `make run-s3`. To run both the binaries use `make run`

### Pre-requisite for S3 example code
- Make sure you have configured a bucket in S3 with the correct permissions and named it `slatedb-bucket`, if you want to use another name then you will have to change it in the `examples/s3.c` file.
- Generate access key for your user that will be attached to the policy and set the following env vars
```
export AWS_ACCESS_KEY_ID=<your-key-id>
export AWS_SECRET_ACCESS_KEY=<your-secret-key>
export AWS_DEFAULT_REGION=<your-bucket-region>
```