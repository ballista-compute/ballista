use std::sync::Arc;
use std::{any::Any, pin::Pin};

use crate::client::BallistaClient;
use crate::memory_stream::MemoryStream;
use crate::scheduler::planner::PartitionLocation;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion::{
    error::{DataFusionError, Result},
    physical_plan::RecordBatchStream,
};
use log::info;

/// UnresolvedShuffleExec represents a dependency on the results of several QueryStageExec nodes which haven't been computed yet.
///
/// An ExecutionPlan that contains an UnresolvedShuffleExec isn't ready for execution. The presence of this ExecutionPlan
/// is used as a signal so the scheduler knows it can't start computation on a specific QueryStageExec.
#[derive(Debug, Clone)]
pub struct UnresolvedShuffleExec {
    // The query stage ids which needs to be computed
    pub(crate) query_stage_ids: Vec<usize>,

    // The schema this node will have once it is replaced with a ShuffleReaderExec
    pub(crate) schema: SchemaRef,

    // The partition count this node will have once it is replaced with a ShuffleReaderExec
    pub(crate) partition_count: usize,
}

impl UnresolvedShuffleExec {
    /// Create a new UnresolvedShuffleExec
    pub fn new(query_stage_ids: Vec<usize>, schema: SchemaRef, partition_count: usize) -> Self {
        Self {
            query_stage_ids,
            schema,
            partition_count,
        }
    }
}

#[async_trait]
impl ExecutionPlan for UnresolvedShuffleExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.partition_count)
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        &self,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan(
            "Ballista UnresolvedShuffleExec does not support with_new_children()".to_owned(),
        ))
    }

    async fn execute(
        &self,
        _partition: usize,
    ) -> Result<Pin<Box<dyn RecordBatchStream + Send + Sync>>> {
        Err(DataFusionError::Plan(
            "Ballista UnresolvedShuffleExec does not support execution".to_owned(),
        ))
    }
}
