use crate::logicalplan::LogicalPlan;

#[derive(Debug, Clone)]
pub enum Action {
    Collect { plan: LogicalPlan },
    //TODO: Write, Repartition, etc
}
