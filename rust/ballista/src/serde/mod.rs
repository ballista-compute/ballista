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

use crate::error::BallistaError;
use crate::execution::physical_plan::Action;
use crate::protobuf;

use prost::Message;

use std::convert::TryInto;
use std::io::Cursor;

pub mod from_proto;
pub mod to_proto;

pub fn decode_protobuf(bytes: &[u8]) -> Result<Action, BallistaError> {
    let mut buf = Cursor::new(bytes);
    protobuf::Action::decode(&mut buf)
        .map_err(|e| BallistaError::General(format!("{:?}", e)))
        .and_then(|node| node.try_into())
}

#[cfg(test)]
mod tests {
    use crate::arrow::datatypes::{DataType, Field, Schema};
    use crate::datafusion::execution::physical_plan::csv::CsvReadOptions;
    use crate::datafusion::logicalplan::{col, lit_str, Expr, LogicalPlanBuilder};
    use crate::error::Result;
    use crate::execution::physical_plan::Action;
    use crate::protobuf;
    use std::convert::TryInto;

    #[test]
    fn roundtrip() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ]);

        let plan = LogicalPlanBuilder::scan_csv(
            "employee.csv",
            CsvReadOptions::new().schema(&schema).has_header(true),
            None,
        )
        .and_then(|plan| plan.filter(col("state").eq(&lit_str("CO"))))
        .and_then(|plan| plan.project(vec![col("id")]))
        .and_then(|plan| plan.build())
        .unwrap();

        let action = Action::Collect {
            plan: plan.clone(),
            // tables: vec![TableMeta::Csv {
            //     table_name: "employee".to_owned(),
            //     has_header: true,
            //     path: "/foo/bar.csv".to_owned(),
            //     schema: schema.clone(),
            // }],
        };

        let proto: protobuf::Action = action.clone().try_into()?;

        let action2: Action = proto.try_into()?;

        assert_eq!(format!("{:?}", action), format!("{:?}", action2));

        Ok(())
    }

    #[test]
    fn roundtrip_aggregate() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ]);

        let plan = LogicalPlanBuilder::scan_csv(
            "employee.csv",
            CsvReadOptions::new().schema(&schema).has_header(true),
            None,
        )
        .and_then(|plan| plan.aggregate(vec![col("state")], vec![max(col("salary"))]))
        .and_then(|plan| plan.build())
        .unwrap();

        let action = Action::Collect {
            plan: plan.clone(),
            // tables: vec![TableMeta::Csv {
            //     table_name: "employee".to_owned(),
            //     has_header: true,
            //     path: "/foo/bar.csv".to_owned(),
            //     schema: schema.clone(),
            // }],
        };

        let proto: protobuf::Action = action.clone().try_into()?;

        let action2: Action = proto.try_into()?;

        assert_eq!(format!("{:?}", action), format!("{:?}", action2));

        Ok(())
    }

    fn max(expr: Expr) -> Expr {
        Expr::AggregateFunction {
            name: "MAX".to_owned(),
            args: vec![expr],
            return_type: DataType::Float64,
        }
    }
}
