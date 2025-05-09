extern crate cbindgen;

use std::path::Path;

fn main() {
    let header_file = Path::new("include").join("slatedb.h");

    cbindgen::generate(".")
        .expect("Unable to generate bindings")
        .write_to_file(header_file);
}
