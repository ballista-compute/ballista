
use std::process::Command;

use clap::{App, Arg};

fn main() {
    let matches = App::new("Ballista Benchmarking Tool")
        //.version(BALLISTA_VERSION)
        .arg(Arg::with_name("bench")
            .short("b")
            .long("bench")
            .help("Benchmark")
            .takes_value(true))
        .arg(Arg::with_name("path")
            .short("p")
            .long("path")
            .value_name("FILE")
            .help("Path to NYC Taxi data files")
            .takes_value(true))
        .get_matches();


    spark_benchmarks();


}

fn spark_benchmarks() {

    let output = Command::new("sh")
        .arg("-c")
        .arg("docker run ballistacomute/spark-benchmarks:0.2.4-SNAPSHOT")
        .output()
        .expect("failed to execute process");

    println!("{:?}", output.status);
    println!("{:?}", String::from_utf8(output.stderr));
    println!("{:?}", String::from_utf8(output.stdout));

}
