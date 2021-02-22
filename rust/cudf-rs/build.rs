fn main() {
    cxx_build::bridge("src/lib.rs")
        .include("/home/andy/miniconda2/envs/cudf_dev/include/")
        .include("/usr/local/cuda-10.2/targets/x86_64-linux/include/")
        .warnings(false)
        .static_flag(true)
        .file("c/library.cpp")
        .flag_if_supported("-std=c++14")
        .flag("-Wno-deprecated-declarations")
        .compile("cudf_c");

    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=c/library.hpp");
    println!("cargo:rerun-if-changed=c/library.cpp");
    println!("cargo:rustc-link-lib=cudf_c");
    println!("cargo:rustc-link-lib=cudf");
    println!("cargo:rustc-link-lib=cuda");
    println!("cargo:rustc-link-lib=cudart");
    println!("cargo:rustc-link-lib=arrow");

    println!("cargo:rustc-link-search=/home/andy/miniconda2/envs/cudf_dev/lib");
    println!("cargo:rustc-link-search=/home/andy/miniconda2/envs/cudf10_2_dev/lib/");
}
