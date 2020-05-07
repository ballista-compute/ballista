use std::env;
use std::process::Command;

use clap::{App, Arg};
use std::collections::HashMap;

#[derive(Debug)]
struct BenchmarkConfig {
    pub name: String,
    pub cpus: usize,
    pub memory_mb: usize,
    pub iterations: usize,
    pub format: String,
}

impl BenchmarkConfig {
    fn result_file(&self) -> String {
        format!("/results/{}-{}-{}cpu-{}mb.txt", self.name, self.format, self.cpus, self.memory_mb)
    }
}

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
        .arg(Arg::with_name("format")
            .required(true)
            .short("f")
            .long("format")
            .help("File format (csv or parquet)")
            .takes_value(true))
        .arg(Arg::with_name("cpus")
            .required(true)
            .short("c")
            .long("cpus")
            .help("CPUs to use")
            .takes_value(true))
        .get_matches();

    let bench = matches.value_of("bench").unwrap();
    let format = matches.value_of("format").unwrap();
    let path = matches.value_of("path").unwrap();
    let cpus = matches.value_of("cpus").unwrap().parse::<usize>().unwrap();

    let version = "0.2.5-SNAPSHOT";

    match bench {
        "spark" => {
            let image = format!("ballistacompute/spark-benchmarks:{}", version);
            let sql = "SELECT passenger_count, MIN(fare_amount), MAX(fare_amount), SUM(fare_amount) FROM tripdata GROUP BY passenger_count";

            let config = BenchmarkConfig {
                name: "spark".to_owned(),
                cpus,
                format: format.to_owned(),
                memory_mb: 2048,
                iterations: 1,
            };
            println!("Running {:?}", config);
            spark_localmode_benchmarks(&image, path, sql, &config);
        }
        "rust" => {
            let image = format!("ballistacompute/rust-benchmarks:{}", version);

            let config = BenchmarkConfig {
                name: "rust".to_owned(),
                cpus,
                format: format.to_owned(),
                memory_mb: 2048,
                iterations: 1,
            };
            println!("Running {:?}", config);
            ballista_rust_localmode_benchmarks(&image, path, &config);
        }
        "jvm" => {
            let image = format!("ballistacompute/jvm-benchmarks:{}", version);

            let config = BenchmarkConfig {
                name: "jvm".to_owned(),
                cpus,
                format: format.to_owned(),
                memory_mb: 2048,
                iterations: 1,
            };
            println!("Running {:?}", config);
            ballista_jvm_localmode_benchmarks(&image, path, &config);
        }
        _ => {
            println!("Invalid bench name");
        }
    }
}

fn ballista_jvm_localmode_benchmarks(image: &str, host_path: &str, config: &BenchmarkConfig) {
    let current_dir = env::current_dir().unwrap();
    let pwd = current_dir.to_str().unwrap();

    let mut volumes = HashMap::new();
    let container_path = "/mnt/nyctaxi".to_owned();
    volumes.insert(host_path, container_path.to_owned());
    volumes.insert(pwd, "/results".to_owned());

    let mut env = HashMap::new();
    env.insert("BENCH_MODE", "local".to_owned());
    env.insert("BENCH_FORMAT", config.format.to_owned());
    env.insert("BENCH_PATH", container_path.to_owned());
    env.insert("BENCH_RESULT_FILE", config.result_file());
    env.insert("BENCH_ITERATIONS", format!("{}", config.iterations));

    //TODO assert result file does not exist

    docker_run(image, &env, &volumes, config);

    //TODO assert result file exists
}

fn ballista_rust_localmode_benchmarks(image: &str, host_path: &str, config: &BenchmarkConfig) {
    let current_dir = env::current_dir().unwrap();
    let pwd = current_dir.to_str().unwrap();

    let mut volumes = HashMap::new();
    let container_path = "/mnt/nyctaxi".to_owned();
    volumes.insert(host_path, container_path.to_owned());
    volumes.insert(pwd, "/results".to_owned());

    let mut env = HashMap::new();
    env.insert("BENCH_MODE", "local".to_owned());
    env.insert("BENCH_FORMAT", config.format.to_owned());
    env.insert("BENCH_PATH", container_path.to_owned());
    env.insert("BENCH_RESULT_FILE", config.result_file());
    env.insert("BENCH_ITERATIONS", format!("{}", config.iterations));

    //TODO assert result file does not exist

    docker_run(image, &env, &volumes, config);

    //TODO assert result file exists
}

fn spark_localmode_benchmarks(image: &str, host_path: &str, sql: &str, config: &BenchmarkConfig) {
    let current_dir = env::current_dir().unwrap();
    let pwd = current_dir.to_str().unwrap();

    let mut volumes = HashMap::new();
    let container_path = "/mnt/nyctaxi".to_owned();
    volumes.insert(host_path, container_path.to_owned());
    volumes.insert(pwd, "/results".to_owned());

    let mut env = HashMap::new();
    env.insert("BENCH_FORMAT", config.format.to_owned());
    env.insert("BENCH_PATH", container_path.to_owned());
    env.insert("BENCH_SQL", sql.to_owned());
    env.insert("BENCH_RESULT_FILE", config.result_file());
    env.insert("BENCH_ITERATIONS", format!("{}", config.iterations));

    //TODO assert result file does not exist

    docker_run(image, &env, &volumes, config);

    //TODO assert result file exists
}

fn docker_run(image: &str, env: &HashMap<&str, String>, volumes: &HashMap<&str, String>, config: &BenchmarkConfig) {
    let mut cmd = "docker run ".to_owned();

    let env_args: Vec<String> = env.iter().map(|(k, v)| format!("-e {}=\"{}\"", k, v)).collect();
    let volume_args: Vec<String> = volumes.iter().map(|(k, v)| format!("-v {}:{}", k, v)).collect();

    cmd += &format!("--cpus={} ", config.cpus);

    // Rust didn't like this
    // cmd += &format!("--memory={}M ", config.memory_mb);
    // cmd += &format!("--memory-swap={}M ", config.memory_mb + 1024);

    cmd += &env_args.join(" ");
    cmd += " ";
    cmd += &volume_args.join(" ");
    cmd += " ";
    cmd += image;

    println!("Executing: {}", cmd);

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
