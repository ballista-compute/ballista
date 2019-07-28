//! Ballista CLI

use clap::{App, Arg, ArgMatches, SubCommand};
use std::time::Instant;

use ballista::cluster;

pub fn main() {
    ::env_logger::init();

    let now = Instant::now();

    let matches = App::new("Ballista")
        .version("0.1.0")
        .author("Andy Grove <andygrove73@gmail.com>")
        .about("Distributed compute platform")
        .subcommand(
            SubCommand::with_name("create-cluster")
                .about("Create a ballista cluster")
                .arg(
                    Arg::with_name("name")
                        .required(true)
                        .takes_value(true)
                        .short("n")
                        .long("name")
                        .help("Ballista cluster name"),
                )
                .arg(
                    Arg::with_name("executors")
                        .short("e")
                        .long("num-executors")
                        .required(true)
                        .takes_value(true)
                        .help("number of executor pods to create"),
                )
                .arg(
                    Arg::with_name("volumes")
                        .required(false)
                        .multiple(true)
                        .takes_value(true)
                        .short("v")
                        .long("volumes")
                        .help("Persistent Volumes to mount into the executor pods. Syntax should be '<pv-name>:<mount-path>'"),
                )
                .arg(
                    Arg::with_name("image")
                        .required(false)
                        .takes_value(true)
                        .short("i")
                        .long("image")
                        .help("Custom Ballista Docker image to use for executors"),
                ),
        )
        .subcommand(
            SubCommand::with_name("delete-cluster")
                .about("Delete a ballista cluster")
                .arg(
                    Arg::with_name("name")
                        .required(true)
                        .takes_value(true)
                        .short("n")
                        .long("name")
                        .help("Ballista cluster name"),
                ),
        )
        .subcommand(
            SubCommand::with_name("run")
                .about("Execute a ballista application")
                .arg(
                    Arg::with_name("name")
                        .required(true)
                        .takes_value(true)
                        .short("n")
                        .long("name")
                        .help("Ballista cluster name"),
                )
                .arg(
                    Arg::with_name("image")
                        .required(true)
                        .takes_value(true)
                        .short("i")
                        .long("image")
                        .help("Docker image for application pod"),
                ),
        )
        .get_matches();

    match matches.subcommand() {
        ("create-cluster", Some(subcommand_matches)) => create_cluster(subcommand_matches),
        ("delete-cluster", Some(subcommand_matches)) => delete_cluster(subcommand_matches),
        ("run", Some(subcommand_matches)) => execute(subcommand_matches),
        _ => {
            println!("Invalid subcommand");
            std::process::exit(-1);
        }
    }

    println!(
        "Executed subcommand {} in {} seconds",
        matches.subcommand_name().unwrap(),
        now.elapsed().as_millis() as f64 / 1000.0
    );
}

fn create_cluster(matches: &ArgMatches<'_>) {
    let cluster_name = matches.value_of("name").unwrap().to_string();
    let replicas = matches
        .value_of("executors")
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let volumes = matches
        .values_of("volumes")
        .map(|v| v.into_iter().map(|i| i.to_string()).collect());
    let image = matches.value_of("image").map(|i| i.to_string());
    let namespace = "default".to_string();

    cluster::ClusterBuilder::new(cluster_name, namespace, replicas)
        .image(image)
        .volumes(volumes)
        .create()
        .expect("Could not create cluster");
}

fn delete_cluster(matches: &ArgMatches<'_>) {
    let cluster_name = matches.value_of("name").unwrap();
    let namespace = "default";
    cluster::delete_cluster(cluster_name, namespace).expect("Could not delete cluster");
}

fn execute(matches: &ArgMatches<'_>) {
    let cluster_name = matches.value_of("name").unwrap();
    let image = matches.value_of("image").unwrap();
    let namespace = "default";

    let pod_name = format!("ballista-{}-app", cluster_name);

    cluster::create_ballista_application(namespace, pod_name, image.to_string()).unwrap();
}
