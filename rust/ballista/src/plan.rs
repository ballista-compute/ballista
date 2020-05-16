use crate::logicalplan::LogicalPlan;

#[derive(Debug, Clone)]
pub enum Action {
    Collect { plan: LogicalPlan },
    WriteCsv { plan: LogicalPlan, path: String },
    WriteParquet { plan: LogicalPlan, path: String },
}
