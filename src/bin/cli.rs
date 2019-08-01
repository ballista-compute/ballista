//! Ballista CLI

use std::time::Instant;
use structopt::StructOpt;

use ballista::cluster;

#[derive(Debug, StructOpt)]
#[structopt(name = "ballista", about = "Distributed compute platform")]
pub struct Opt {
    #[structopt(subcommand)]
    pub cmd: Command,
}

#[derive(Debug, StructOpt)]
pub enum Command {
    #[structopt(name = "create-cluster", about = "Create a ballista cluster")]
    CreateCluster(CreateCluster),

    #[structopt(name = "delete-cluster", about = "Delete a ballista cluster")]
    DeleteCluster(DeleteCluster),

    #[structopt(name = "run", about = "Execute a Ballista application")]
    Run(Run),
}

#[derive(Debug, StructOpt)]
pub struct CreateCluster {
    #[structopt(short = "n", long = "name", help = "Ballista cluster name")]
    pub name: String,

    #[structopt(
        short = "e",
        long = "num-executors",
        help = "number of executor pods to create"
    )]
    pub num_executors: usize,

    #[structopt(
        short = "v",
        long = "volumes",
        help = "Persistent Volumes to mount into the executor pods. Syntax should be '<pv-name>:<mount-path>'"
    )]
    pub volumes: Vec<String>,

    #[structopt(
        name = "image",
        short = "i",
        help = "Custom Ballista Docker image to use for executors"
    )]
    pub image: Option<String>,
}

#[derive(Debug, StructOpt)]
pub struct DeleteCluster {
    #[structopt(short = "n", long = "name", help = "Ballista cluster name")]
    pub name: String,
}

#[derive(Debug, StructOpt)]
pub struct Run {
    #[structopt(short = "n", long = "name", help = "Ballista cluster name")]
    pub name: String,

    #[structopt(name = "image", short = "i", help = "Docker image for application pod")]
    pub image: String,
}

pub fn main() {
    ::env_logger::init();

    let now = Instant::now();
    let opt = Opt::from_args();
    let subcommand = match opt.cmd {
        Command::CreateCluster(c) => {
            create_cluster(c);
            "create-cluster"
        }
        Command::DeleteCluster(d) => {
            delete_cluster(d);
            "delete-cluster"
        }
        Command::Run(r) => {
            execute(r);
            "run"
        }
    };

    println!(
        "Executed subcommand {} in {} seconds",
        subcommand,
        now.elapsed().as_millis() as f64 / 1000.0
    );
}

fn create_cluster(args: CreateCluster) {
    let volumes = if args.volumes.is_empty() {
        None
    } else {
        Some(args.volumes)
    };
    let namespace = "default".to_string();

    cluster::ClusterBuilder::new(args.name, namespace, args.num_executors)
        .image(args.image)
        .volumes(volumes)
        .create()
        .expect("Could not create cluster");
}

fn delete_cluster(args: DeleteCluster) {
    let namespace = "default";
    cluster::delete_cluster(&args.name, namespace).expect("Could not delete cluster");
}

fn execute(args: Run) {
    let namespace = "default";

    let pod_name = format!("ballista-{}-app", args.name);

    cluster::create_ballista_application(namespace, pod_name, args.image).unwrap();
}
