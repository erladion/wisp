// Link against libwisp (the C ABI, common/connectionapi.h).
//
// The library is located, in order of preference:
//   1. $WISP_LIB_DIR                — explicit override
//   2. ../../build/common           — the repo's default CMake build tree
// then the crate links `-lwisp` dynamically. At run time libwisp.so must be on
// the loader path (rpath, LD_LIBRARY_PATH, or an installed prefix).

use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-env-changed=WISP_LIB_DIR");

    let search = env::var("WISP_LIB_DIR").map(PathBuf::from).unwrap_or_else(|_| {
        let manifest = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
        // bindings/rust -> repo root -> build/common
        manifest.join("../../build/common")
    });

    println!("cargo:rustc-link-search=native={}", search.display());
    println!("cargo:rustc-link-lib=dylib=wisp");
}
