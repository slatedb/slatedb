// println! is required in build scripts for cargo directives
#![allow(clippy::disallowed_macros)]

use cmake::Config;
use std::env::var;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    let out_dir = PathBuf::from(require_env("OUT_DIR"));
    let manifest_dir = PathBuf::from(require_env("CARGO_MANIFEST_DIR"));
    let schemas_dir = match manifest_dir.parent() {
        Some(parent) => parent.join("schemas"),
        None => {
            println!("cargo::error=Failed to get parent directory of CARGO_MANIFEST_DIR");
            return;
        }
    };

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

    let flatc_path = build_flatc();

    if !generate_flatbuffers(&flatc_path, &schemas_dir, &out_dir) {
        return;
    }

    verify_generated_files_exist(&out_dir);
}

/// Get an environment variable or print a cargo error.
fn require_env(name: &str) -> String {
    match var(name) {
        Ok(val) => val,
        Err(_) => {
            println!("cargo::error={} environment variable not set", name);
            std::process::exit(1);
        }
    }
}

/// Build flatc from the git submodule `flatbuffers` using cmake and
/// return the path to the executable.
fn build_flatc() -> PathBuf {
    let mut config = Config::new("../flatbuffers");

    // warning C4530: C++ exception handler used, but unwind semantics are not enabled. Specify /EHsc
    let target = require_env("TARGET");
    let host = require_env("HOST");
    if target.contains("msvc") && host.contains("windows") {
        config.cxxflag("/EHsc");
    }

    let dst = config.build();

    if cfg!(windows) {
        dst.join("bin").join("flatc.exe")
    } else {
        dst.join("bin").join("flatc")
    }
}

/// Generate flatbuffers code into `OUT_DIR`. Returns true on success.
fn generate_flatbuffers(
    flatc_path: &std::path::Path,
    schemas_dir: &std::path::Path,
    out_dir: &std::path::Path,
) -> bool {
    let root_schema = schemas_dir.join("root.fbs");

    if !root_schema.exists() {
        println!("cargo::error=Schema file not found: {:?}", root_schema);
        return false;
    }

    let output = match Command::new(flatc_path)
        .arg("-o")
        .arg(out_dir)
        .arg("--rust")
        .arg("--gen-all")
        .arg(&root_schema)
        .output()
    {
        Ok(output) => output,
        Err(e) => {
            println!("cargo::error=Failed to run flatc: {}", e);
            return false;
        }
    };

    if !output.status.success() {
        println!(
            "cargo::error=flatc failed:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return false;
    }

    true
}

/// Verifies whether the generated file exists
fn verify_generated_files_exist(out_dir: &std::path::Path) {
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
    }
}
