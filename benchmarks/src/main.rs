use std::env;
use std::process::Command;

use clap::{App, Arg};
use std::collections::HashMap;

fn main() {
    let matches = App::new("Ballista Benchmarking Tool")
        //.version(BALLISTA_VERSION)
        .arg(Arg::with_name("bench")
            .required(true)
            .short("b")
            .long("bench")
            .help("Benchmark")
            .takes_value(true))
        .arg(Arg::with_name("path")
            .required(true)
            .short("p")
            .long("path")
            .value_name("FILE")
            .help("Path to NYC Taxi data files")
            .takes_value(true))
        .get_matches();

    let bench = matches.value_of("bench").expect("");

    let version = "0.2.4-SNAPSHOT";

    match bench {
        "spark" => {
            let image = format!("ballistacompute/spark-benchmarks:{}", version);
            let format = "csv";
            let sql = "SELECT passenger_count, MIN(fare_amount), MAX(fare_amount), SUM(fare_amount) FROM tripdata GROUP BY passenger_count";
            let path = "/mnt/nyctaxi/csv/year=2019";
            spark_benchmarks(&image, format, path, sql);
        }
        _ => {
            unimplemented!()
        }
    }
}

fn spark_benchmarks(image: &str, format: &str, host_path: &str, sql: &str) {
    let current_dir = env::current_dir().unwrap();
    let pwd = current_dir.to_str().unwrap();

    let mut volumes = HashMap::new();
    let container_path = "/mnt/nyctaxi";
    volumes.insert(host_path, container_path);
    volumes.insert(pwd, "/results");

    let mut env = HashMap::new();
    env.insert("BENCH_FORMAT", format);
    env.insert("BENCH_PATH", container_path);
    env.insert("BENCH_SQL", sql);
    env.insert("BENCH_RESULT_FILE", "/results/spark-query1-csv.txt");

    //TODO assert result file does not exist

    docker_run(image, &env, &volumes);

    //TODO assert result file exists
}

fn docker_run(image: &str, env: &HashMap<&str,&str>, volumes: &HashMap<&str,&str>) {
    let mut cmd = "docker run ".to_owned();

    let env_args: Vec<String> = env.iter().map(|(k,v)| format!("-e {}=\"{}\"", k, v)).collect();
    let volume_args: Vec<String> = volumes.iter().map(|(k,v)| format!("-v {}:{}", k, v)).collect();

    cmd += &env_args.join(" ");
    cmd += " ";
    cmd += &volume_args.join(" ");
    cmd += " ";
    cmd += image;

    let output = Command::new("sh")
        .arg("-c")
        .arg(&cmd)
        .output()
        .expect("failed to execute process");

    println!("{:?}", output.status);
    println!("{:?}", String::from_utf8(output.stderr));

    //stdout can be large
    // println!("{:?}", String::from_utf8(output.stdout));

    if !output.status.success() {
        panic!("failed")
    }
}
