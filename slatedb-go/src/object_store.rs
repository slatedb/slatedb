use std::sync::Arc;
use std::time::Duration;

use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use slatedb::object_store::{memory::InMemory, ObjectStore};

use object_store::client::ClientOptions;

use crate::config::AwsConfigJson;
use crate::error::{create_error_result, CSdbError, CSdbResult};

// Helper function to create in-memory object store
pub fn create_inmemory_store() -> Result<Arc<dyn ObjectStore>, CSdbResult> {
    Ok(Arc::new(InMemory::new()))
}

// Helper function to create AWS S3 object store
pub async fn create_aws_store(
    aws_config: &AwsConfigJson,
) -> Result<Arc<dyn ObjectStore>, CSdbResult> {
    let bucket_str = if let Some(bucket) = aws_config.bucket.clone() {
        bucket
    } else if let Ok(bucket) = std::env::var("AWS_BUCKET") {
        bucket
    } else {
        return Err(create_error_result(
            CSdbError::InvalidArgument,
            "AWS bucket must be specified in config or AWS_BUCKET environment variable",
        ));
    };

    let region_str = if let Some(region) = aws_config.region.clone() {
        region
    } else if let Ok(region) = std::env::var("AWS_REGION") {
        region
    } else {
        return Err(create_error_result(
            CSdbError::InvalidArgument,
            "AWS region must be specified in config or AWS_REGION environment variable",
        ));
    };

    // Optional AWS credentials - if not provided, will use credential provider chain (IRSA, IMDS, etc.)
    let access_key_id = std::env::var("AWS_ACCESS_KEY_ID").ok();
    let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY").ok();
    let session_token = std::env::var("AWS_SESSION_TOKEN").ok();

    // Configure client options with timeout settings
    let mut client_options = ClientOptions::new();

    // Apply request timeout setting from AWS config if provided
    if let Some(timeout_ns) = aws_config.request_timeout {
        if timeout_ns > 0 {
            client_options = client_options.with_timeout(Duration::from_nanos(timeout_ns));
        }
    }

    // Build the S3 object store with required parameters
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(bucket_str)
        .with_region(region_str)
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .with_client_options(client_options);

    // Handle optional endpoint for S3-compatible storage
    if let Some(endpoint_str) = aws_config
        .endpoint
        .clone()
        .or_else(|| std::env::var("AWS_ENDPOINT").ok())
    {
        builder = builder.with_endpoint(endpoint_str);
    }

    // If explicit credentials are supplied, configure them; otherwise rely on the AWS SDK
    // default credential provider chain (which covers IMDS / IRSA).
    if let (Some(access_key), Some(secret_key)) = (access_key_id, secret_access_key) {
        builder = builder
            .with_access_key_id(access_key)
            .with_secret_access_key(secret_key);

        if let Some(token) = session_token {
            builder = builder.with_token(token);
        }
    }

    match builder.build() {
        Ok(store) => Ok(Arc::new(store)),
        Err(e) => {
            let error_msg = format!("Failed to create AWS object store: {}", e);
            Err(create_error_result(CSdbError::InternalError, &error_msg))
        }
    }
}
