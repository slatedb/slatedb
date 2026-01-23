// println! is required in build scripts for cargo directives
#![allow(clippy::disallowed_macros)]

use std::env;
use std::fs::{self, File};
use std::io::{self, Cursor, Read};
use std::path::{Path, PathBuf};
use std::process::Command;

const FLATC_VERSION: &str = "24.3.25";

fn main() {
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR not set"));
    let flatc_dir = out_dir.join("flatc");
    let manifest_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set"));
    let schemas_dir = manifest_dir
        .parent()
        .expect("Failed to get parent directory")
        .join("schemas");
    let generated_dir = manifest_dir.join("src").join("generated");

    // Tell Cargo to rerun if any schema file changes
    println!("cargo:rerun-if-changed={}", schemas_dir.display());
    if let Ok(entries) = fs::read_dir(&schemas_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "fbs") {
                println!("cargo:rerun-if-changed={}", path.display());
            }
        }
    }

    // Download and extract flatc if not already present
    let flatc_path = get_or_download_flatc(&flatc_dir);

    // Generate flatbuffer code into src/generated
    generate_flatbuffers(&flatc_path, &schemas_dir, &generated_dir);
}

fn get_or_download_flatc(flatc_dir: &Path) -> PathBuf {
    let flatc_binary = if cfg!(target_os = "windows") {
        flatc_dir.join("flatc.exe")
    } else {
        flatc_dir.join("flatc")
    };

    // Check if we already have the correct version
    if flatc_binary.exists() {
        if let Ok(output) = Command::new(&flatc_binary).arg("--version").output() {
            let version_output = String::from_utf8_lossy(&output.stdout);
            if version_output.contains(FLATC_VERSION) {
                return flatc_binary;
            }
        }
    }

    // Download flatc
    fs::create_dir_all(flatc_dir).expect("Failed to create flatc directory");

    let (url, binary_name) = get_download_url();
    println!(
        "cargo:warning=Downloading flatc {} from {}",
        FLATC_VERSION, url
    );

    let response = ureq::get(&url).call().expect("Failed to download flatc");

    let mut bytes = Vec::new();
    response
        .into_reader()
        .read_to_end(&mut bytes)
        .expect("Failed to read response body");

    // Extract the archive
    extract_archive(&bytes, &binary_name, &flatc_binary);

    // Make executable on Unix
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&flatc_binary)
            .expect("Failed to get flatc metadata")
            .permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&flatc_binary, perms).expect("Failed to set flatc permissions");
    }

    // Verify the binary works
    let output = Command::new(&flatc_binary)
        .arg("--version")
        .output()
        .expect("Failed to run flatc --version");

    if !output.status.success() {
        panic!(
            "flatc --version failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    println!(
        "cargo:warning=flatc version: {}",
        String::from_utf8_lossy(&output.stdout).trim()
    );

    flatc_binary
}

fn get_download_url() -> (String, String) {
    let base_url = format!(
        "https://github.com/google/flatbuffers/releases/download/v{}",
        FLATC_VERSION
    );

    let (archive_name, binary_name) = if cfg!(target_os = "macos") {
        ("Mac.flatc.binary.zip", "flatc")
    } else if cfg!(target_os = "linux") {
        ("Linux.flatc.binary.clang++-15.zip", "flatc")
    } else if cfg!(target_os = "windows") {
        ("Windows.flatc.binary.zip", "flatc.exe")
    } else {
        panic!("Unsupported operating system");
    };

    (
        format!("{}/{}", base_url, archive_name),
        binary_name.to_string(),
    )
}

fn extract_archive(bytes: &[u8], binary_name: &str, flatc_binary: &Path) {
    let cursor = Cursor::new(bytes);
    let mut archive = zip::ZipArchive::new(cursor).expect("Failed to open zip archive");

    // Collect file names first to avoid borrow issues
    let file_names: Vec<String> = (0..archive.len())
        .filter_map(|i| archive.by_index(i).ok().map(|f| f.name().to_string()))
        .collect();

    // Look for the flatc binary in the archive
    for (i, name) in file_names.iter().enumerate() {
        if name.ends_with(binary_name) || name == binary_name {
            let mut file = archive.by_index(i).expect("Failed to read archive entry");
            let mut output_file =
                File::create(flatc_binary).expect("Failed to create flatc binary file");
            io::copy(&mut file, &mut output_file).expect("Failed to extract flatc binary");
            return;
        }
    }

    // If we didn't find it with the expected name, try to find any flatc binary
    for (i, name) in file_names.iter().enumerate() {
        if name.contains("flatc") && !name.ends_with('/') {
            let mut file = archive.by_index(i).expect("Failed to read archive entry");
            let mut output_file =
                File::create(flatc_binary).expect("Failed to create flatc binary file");
            io::copy(&mut file, &mut output_file).expect("Failed to extract flatc binary");
            return;
        }
    }

    panic!(
        "Could not find flatc binary in archive. Archive contents: {:?}",
        file_names
    );
}

fn generate_flatbuffers(flatc_path: &Path, schemas_dir: &Path, out_dir: &Path) {
    let root_schema = schemas_dir.join("root.fbs");

    if !root_schema.exists() {
        panic!("Schema file not found: {:?}", root_schema);
    }

    println!(
        "cargo:warning=Generating flatbuffers from {:?}",
        root_schema
    );

    let output = Command::new(flatc_path)
        .arg("-o")
        .arg(out_dir)
        .arg("--rust")
        .arg("--gen-all")
        .arg(&root_schema)
        .output()
        .expect("Failed to run flatc");

    if !output.status.success() {
        panic!(
            "flatc failed:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    // Verify the generated file exists
    let generated_file = out_dir.join("root_generated.rs");
    if !generated_file.exists() {
        panic!(
            "Generated file not found: {:?}. OUT_DIR contents: {:?}",
            generated_file,
            fs::read_dir(out_dir)
                .map(|entries| entries
                    .filter_map(|e| e.ok().map(|e| e.path()))
                    .collect::<Vec<_>>())
                .unwrap_or_default()
        );
    }

    println!(
        "cargo:warning=Generated flatbuffer code at {:?}",
        generated_file
    );
}
