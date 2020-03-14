use std::sync::Arc;

use crate::error::{ballista_error, BallistaError, Result};
use crate::logical_plan::{LogicalExpr, LogicalPlan};

use datafusion::logicalplan::LogicalPlan as DataFusionPlan;

/// Wrapper around DataFusion library
struct DataFusionContext {}

impl DataFusionContext {
    fn create_datafusion_plan(&self, plan: &LogicalPlan) -> Result<DataFusionPlan> {
        match plan {
            // LogicalPlan::Projection { expr, input} => {
            //     DataFusionPlan::Projection {
            //         expr: vec![],
            //         input: Arc::new(create_datafusion_plan(input.as_ref())),
            //         schema: Arc::new(())
            //     }
            // }
            _ => Err(ballista_error("tbd")),
        }
    }
}
