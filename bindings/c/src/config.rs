use std::ffi::c_char;

#[repr(C)]
#[allow(non_camel_case_types)]
pub enum slatedb_config_source {
    DEFAULT,
    FROM_FILE(*const c_char),
    FROM_ENV(*const c_char),
    LOAD,
}

#[repr(C)]
#[allow(non_camel_case_types)]
pub enum slatedb_object_store {
    AWS(slatedb_aws_config),
    IN_MEMORY,
    LOCAL_FILESYSTEM,
    // TODO: add other storage options
}

#[repr(C)]
#[allow(non_camel_case_types)]
pub enum slatedb_s3_conditional_put {
    NONE,
    ETAG_MATCH,
    ETAG_PUT_IF_NOT_EXISTS,
    DYNAMO(*const c_char),
}

#[repr(C)]
pub struct slatedb_aws_config {
    pub bucket_name: *const c_char,
    pub conditional_put: slatedb_s3_conditional_put,
}
