// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

fn main() {
    //TODO remove the hard-coded paths
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
