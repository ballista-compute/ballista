use std::sync::Arc;

use crate::ballista_proto;
use crate::error::{BallistaError, Result};

use arrow::datatypes::Schema;
use datafusion::logicalplan::LogicalPlan as DFPlan;

pub fn create_datafusion_plan(plan: &ballista_proto::LogicalPlanNode) -> Result<DFPlan> {
    if plan.file.is_some() {

        let file = plan.file.as_ref().unwrap();

        Ok(DFPlan::TableScan {
            schema_name: "default".to_string(),
            table_name: file.filename.clone(),
            schema: Arc::new(Schema::new(vec![])),
            projection: None,
        })
    } else if plan.projection.is_some() {
        if let Some(input) = &plan.input {
            Ok(DFPlan::Projection {
                expr: vec![],
                input: Arc::new(create_datafusion_plan(&input)?),
                schema: Arc::new(Schema::new(vec![])),
            })
        } else {
            Err(BallistaError::NotImplemented)
        }
    } else {
        Err(BallistaError::NotImplemented)
    }
}
