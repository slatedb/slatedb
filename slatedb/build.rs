// println! is required in build scripts for cargo directives
#![allow(clippy::disallowed_macros)]

use flatcc::Args;
use std::env::var;
use std::path::PathBuf;

type Result<T> = std::result::Result<T, ()>;

fn main() {
    let out_dir =
        PathBuf::from(require_env("OUT_DIR").expect("Environment variable OUT_DIR should exist."));
    let manifest_dir = PathBuf::from(
        require_env("CARGO_MANIFEST_DIR")
            .expect("Environment variable CARGO_MANIFEST_DIR should exist."),
    );
    let repo_root = match manifest_dir.parent() {
        Some(parent) => parent,
        None => {
            println!("cargo::error=Failed to get parent directory of CARGO_MANIFEST_DIR");
            panic!(
                "Parent directory of CARGO_MANIFEST_DIR: {} should exist",
                manifest_dir.display()
            );
        }
    };
    let schemas_dir = repo_root.join("schemas");

    // Rerun if any schema file changes
    println!("cargo:rerun-if-changed={}", schemas_dir.display());
    if let Ok(entries) = std::fs::read_dir(&schemas_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "fbs") {
                println!("cargo:rerun-if-changed={}", path.display());
            }
        }
    }

    generate_flatbuffers(&schemas_dir, &out_dir)
        .expect("Flatbuffers code should have been generated");
    verify_generated_files_exist(&out_dir).expect("Generated flatbuffers files should exist");
}

/// Get an environment variable or print a cargo error.
fn require_env(name: &str) -> Result<String> {
    match var(name) {
        Ok(val) => Ok(val),
        Err(_) => {
            println!("cargo::error={} environment variable not set", name);
            Err(())
        }
    }
}

/// Generate flatbuffers code into `OUT_DIR`.
fn generate_flatbuffers(schemas_dir: &std::path::Path, out_dir: &std::path::Path) -> Result<()> {
    let root_schema = schemas_dir.join("root.fbs");

    if !root_schema.exists() {
        println!("cargo::error=Schema file not found: {:?}", root_schema);
        return Err(());
    }

    flatcc::Builder::new()
        .run(Args {
            inputs: &[root_schema.as_ref()],
            out_dir,
            extra: &["--gen-all"],
            ..Default::default()
        })
        .map_err(|_| ())
}

/// Verifies whether the generated file exists
fn verify_generated_files_exist(out_dir: &std::path::Path) -> Result<()> {
    let generated_file = out_dir.join("root_generated.rs");
    if !generated_file.exists() {
        println!(
            "cargo::error=Generated file not found: {:?}. OUT_DIR contents: {:?}",
            generated_file,
            std::fs::read_dir(out_dir)
                .map(|entries| entries
                    .filter_map(|e| e.ok().map(|e| e.path()))
                    .collect::<Vec<_>>())
                .unwrap_or_default()
        );
        return Err(());
    }
    Ok(())
}
