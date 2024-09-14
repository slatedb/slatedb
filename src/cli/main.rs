use crate::args::{parse_args, CliArgs, CliCommands};
use object_store::path::Path;
use object_store::ObjectStore;
use slatedb::manifest_store::read_manifest;
use std::env;
use std::error::Error;
use std::sync::Arc;

mod args;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: CliArgs = parse_args();
    let path = Path::from(args.path.as_str());
    let object_store = load_object_store_from_env(args.env_file)?;
    match args.command {
        CliCommands::ReadManifest => exec_read_manifest(&path, object_store).await?,
    }

    Ok(())
}

async fn exec_read_manifest(
    path: &Path,
    object_store: Arc<dyn ObjectStore>,
) -> Result<(), Box<dyn Error>> {
    match read_manifest(path, object_store).await? {
        None => {
            println!("No manifest file found.")
        }
        Some(manifest) => {
            println!("{}", manifest);
        }
    }
    Ok(())
}

fn load_object_store_from_env(
    env_file: Option<String>,
) -> Result<Arc<dyn ObjectStore>, Box<dyn Error>> {
    dotenv::from_filename(env_file.unwrap_or(String::from(".env"))).ok();

    let provider = &*env::var("OS_PROVIDER")
        .expect("OS_PROVIDER must be set")
        .to_lowercase();
    match provider {
        "aws" => load_aws(),
        _ => Err(format!("Unknown OS_PROVIDER: '{}'", provider).into()),
    }
}

fn load_aws() -> Result<Arc<dyn ObjectStore>, Box<dyn Error>> {
    #[cfg(not(feature = "aws"))]
    panic!("feature 'aws' must be enabled to use OS_PROVIDER=aws");

    let key = env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID must be set");
    let secret =
        env::var("AWS_SECRET_ACCESS_KEY").expect("Expected AWS_SECRET_ACCESS_KEY must be set");
    let bucket = env::var("AWS_BUCKET").expect("AWS_BUCKET must be set");
    let region = env::var("AWS_REGION").expect("AWS_REGION must be set");

    Ok(Arc::new(
        object_store::aws::AmazonS3Builder::new()
            .with_access_key_id(key)
            .with_secret_access_key(secret)
            .with_bucket_name(bucket)
            .with_region(region)
            .build()?,
    ) as Arc<dyn ObjectStore>)
}
