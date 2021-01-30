// Copyright 2020 Andy Grove
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod from_proto;
pub mod to_proto;

#[cfg(test)]
mod roundtrip_tests {
    use std::{convert::TryInto, sync::Arc};
    use datafusion::physical_plan::hash_utils::JoinType;

    use arrow::datatypes::Schema;
    use datafusion::physical_plan::{
        empty::EmptyExec,
        limit::{GlobalLimitExec, LocalLimitExec},
        hash_join::HashJoinExec,
        ExecutionPlan,
    };

    use super::super::super::error::Result;
    use super::super::protobuf;
    use futures::io::Empty;

    fn roundtrip_test(exec_plan: Arc<dyn ExecutionPlan>) -> Result<()> {
        let proto: protobuf::PhysicalPlanNode = exec_plan.clone().try_into()?;
        let result_exec_plan: Arc<dyn ExecutionPlan> = (&proto).try_into()?;
        assert_eq!(
            format!("{:?}", exec_plan),
            format!("{:?}", result_exec_plan)
        );
        Ok(())
    }

    #[test]
    fn roundtrip_empty() -> Result<()> {
        roundtrip_test(Arc::new(EmptyExec::new(false, Arc::new(Schema::empty()))))
    }

    #[test]
    fn roundtrip_local_limit() -> Result<()> {
        roundtrip_test(Arc::new(LocalLimitExec::new(
            Arc::new(EmptyExec::new(false, Arc::new(Schema::empty()))),
            25,
        )))
    }

    #[test]
    fn roundtrip_global_limit() -> Result<()> {
        roundtrip_test(Arc::new(GlobalLimitExec::new(
            Arc::new(EmptyExec::new(false, Arc::new(Schema::empty()))),
            25,
            0,
        )))
    }

    #[test]
    fn roundtrip_hash_join() -> Result<()> {
        use arrow::datatypes::{Field, DataType, Schema};
         let field_a = Field::new("col", DataType::Int64, false);
         let schema_left = Schema::new(vec![field_a.clone()]);
         let schema_right = Schema::new(vec![field_a.clone()]);

        roundtrip_test(Arc::new(HashJoinExec::try_new(
            Arc::new(EmptyExec::new(false, Arc::new(schema_left))),
            Arc::new(EmptyExec::new(false, Arc::new(schema_right))),
            &[("col".to_string(), "col".to_string())],
            &JoinType::Inner,
        )?))
    }
}
