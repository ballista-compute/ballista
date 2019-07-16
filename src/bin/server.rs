#[macro_use]
extern crate log;

use crate::proto::{server, ExecuteRequest, ExecuteResponse, TableMeta};

use futures::{future, Future, Stream};
use log::error;
use tokio::net::TcpListener;
use tower_grpc::{Request, Response};
use tower_hyper::server::{Http, Server};

use arrow::array;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use ballista::error::{BallistaError, Result};
use ballista::execution;
use ballista::proto;
use datafusion::execution::context::ExecutionContext;
use datafusion::logicalplan::LogicalPlan;

#[derive(Clone, Debug)]
struct BallistaService;

impl server::Executor for BallistaService {
    type ExecuteFuture = future::FutureResult<Response<ExecuteResponse>, tower_grpc::Status>;

    fn execute(&mut self, request: Request<ExecuteRequest>) -> Self::ExecuteFuture {
        info!("REQUEST = {:?}", request);
        let request = request.get_ref();

        let response = match &request.plan {
            Some(plan) => match execution::create_datafusion_plan(plan) {
                Ok(df_plan) => match execute_query(
                    &request.table_meta,
                    df_plan.as_ref().to_logical_plan().as_ref(),
                ) {
                    Ok(batches) => Response::new(ExecuteResponse {
                        message: "SUCCESS".to_string(),
                        batch: batches,
                    }),
                    Err(e) => Response::new(ExecuteResponse {
                        message: format!("Error executing plan: {:?}", e),
                        batch: vec![],
                    }),
                },
                Err(e) => Response::new(ExecuteResponse {
                    message: format!("Error converting plan: {:?}", e),
                    batch: vec![],
                }),
            },
            _ => Response::new(ExecuteResponse {
                message: "empty request".to_string(),
                batch: vec![],
            }),
        };

        future::ok(response)
    }
}

fn execute_query(
    table_meta: &Vec<TableMeta>,
    df_plan: &LogicalPlan,
) -> Result<Vec<proto::RecordBatch>> {
    let mut context = ExecutionContext::new();
    table_meta.iter().for_each(|table| {
        let schema = execution::create_arrow_schema(table.schema.as_ref().unwrap()).unwrap();
        info!(
            "Registering table {} as filename {}",
            table.table_name, table.filename
        );
        context.register_csv(&table.table_name, &table.filename, &schema, true); //TODO has_header should not be hard-coded
    });

    // the plan is already optimized by the client!

    //    let optimized_plan = context.optimize(&df_plan)?;
    //    info!("Optimized plan: {:?}", optimized_plan);

    println!("Executing: {:?}", df_plan);

    let relation = context.execute(&df_plan, 1024)?;

    let mut x = relation.borrow_mut();

    let mut batches = vec![];
    while let Some(batch) = x.next()? {
        println!(
            "Reading batch with {} rows x {} columns",
            batch.num_rows(),
            batch.num_columns()
        );
        batches.push(serialize_batch(&batch)?);
    }

    Ok(batches)
}

fn serialize_batch(batch: &RecordBatch) -> Result<proto::RecordBatch> {
    // this just serializes to CSV for now but needs to use IPC encoding instead
    let mut data = String::new();
    for i in 0..batch.num_rows() {
        for j in 0..batch.num_columns() {
            if j > 0 {
                data.push_str(",");
            }

            let col = batch.column(j).as_any();

            match batch.schema().field(j).data_type() {
                DataType::Utf8 => {
                    let col = col.downcast_ref::<array::BinaryArray>().unwrap();
                    data.push_str(&format!("{:?}", col.value(i)));
                }
                DataType::UInt8 => {
                    let col = col.downcast_ref::<array::UInt8Array>().unwrap();
                    data.push_str(&format!("{}", col.value(i)));
                }
                DataType::UInt16 => {
                    let col = col.downcast_ref::<array::UInt16Array>().unwrap();
                    data.push_str(&format!("{}", col.value(i)));
                }
                DataType::UInt32 => {
                    let col = col.downcast_ref::<array::UInt32Array>().unwrap();
                    data.push_str(&format!("{}", col.value(i)));
                }
                DataType::UInt64 => {
                    let col = col.downcast_ref::<array::UInt64Array>().unwrap();
                    data.push_str(&format!("{}", col.value(i)));
                }
                DataType::Int8 => {
                    let col = col.downcast_ref::<array::Int8Array>().unwrap();
                    data.push_str(&format!("{}", col.value(i)));
                }
                DataType::Int16 => {
                    let col = col.downcast_ref::<array::Int16Array>().unwrap();
                    data.push_str(&format!("{}", col.value(i)));
                }
                DataType::Int32 => {
                    let col = col.downcast_ref::<array::Int32Array>().unwrap();
                    data.push_str(&format!("{}", col.value(i)));
                }
                DataType::Int64 => {
                    let col = col.downcast_ref::<array::Int64Array>().unwrap();
                    data.push_str(&format!("{}", col.value(i)));
                }
                DataType::Float32 => {
                    let col = col.downcast_ref::<array::Float32Array>().unwrap();
                    data.push_str(&format!("{}", col.value(i)));
                }
                DataType::Float64 => {
                    let col = col.downcast_ref::<array::Float64Array>().unwrap();
                    data.push_str(&format!("{}", col.value(i)));
                }
                other => {
                    return Err(BallistaError::NotImplemented(format!(
                        "Unsupported result data type: {:?}",
                        other
                    )))
                }
            }
        }
        data.push_str("\n");
    }
    Ok(proto::RecordBatch { data })
}

pub fn main() {
    let _ = ::env_logger::init();

    let new_service = server::ExecutorServer::new(BallistaService);

    let mut server = Server::new(new_service);

    let http = Http::new().http2_only(true).clone();

    let addr = "0.0.0.0:9090".parse().unwrap();
    info!("Ballista server binding to {}", addr);
    let bind = TcpListener::bind(&addr).expect("bind");

    let serve = bind
        .incoming()
        .for_each(move |sock| {
            if let Err(e) = sock.set_nodelay(true) {
                return Err(e);
            }

            let serve = server.serve_with(sock, http.clone());
            tokio::spawn(serve.map_err(|e| error!("hyper error: {:?}", e)));

            Ok(())
        })
        .map_err(|e| error!("accept error: {}", e));

    info!("Ballista running");
    tokio::run(serve)
}
