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

    use super::super::super::error::Result;
    use super::super::protobuf;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_plan::{LogicalPlan, LogicalPlanBuilder};
    use datafusion::physical_plan::csv::CsvReadOptions;
    use datafusion::prelude::*;
    use core::panic;
    use std::convert::TryInto;


    //Given a identity of a LogicalPlan converts it to protobuf and back, using debug formatting to test equality.
    macro_rules! roundtrip_test {
        ($initial_struct:ident, $proto_type:ty, $struct_type:ty) => {
            let proto: $proto_type = (&$initial_struct).try_into()?;
            let round_trip: $struct_type = (&proto).try_into()?;
            assert_eq!(format!("{:?}", $initial_struct), format!("{:?}", round_trip));
        };
        ($initial_struct:ident, $struct_type:ty)=>{
            roundtrip_test!($initial_struct, protobuf::LogicalPlanNode, $struct_type);
        };
        ($initial_struct:ident)=>{
            roundtrip_test!($initial_struct, protobuf::LogicalPlanNode, LogicalPlan);
        };
    }


    #[test]
    fn roundtrip_repartition()->Result<()>{
        use datafusion::logical_plan::{Partitioning, Expr};
        let test_batch_sizes = [usize::MIN, usize::MAX, 43256];
        let test_expr: Vec<Expr> = vec![Expr::Column("c1".to_string()) + Expr::Column("c2".to_string()), Expr::Literal((4.0).into()) ];
        
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ]);

        let plan = std::sync::Arc::new(LogicalPlanBuilder::scan_csv(
            "employee.csv",
            CsvReadOptions::new().schema(&schema).has_header(true),
            Some(vec![3, 4]),
        ).and_then(|plan| plan.sort(vec![col("salary")]))
        .and_then(|plan| plan.build()).unwrap());
        
        
        
        for batch_size in test_batch_sizes.iter(){
            let rr_repartition = Partitioning::RoundRobinBatch(*batch_size);
            let roundtrip_plan = LogicalPlan::Repartition{input: plan.clone(), partitioning_scheme: rr_repartition};
            roundtrip_test!(roundtrip_plan);

            let h_repartition = Partitioning::Hash(test_expr.clone(), *batch_size);
            let roundtrip_plan = LogicalPlan::Repartition{input: plan.clone(), partitioning_scheme: h_repartition};

            roundtrip_test!(roundtrip_plan);
            
            let no_expr_hrepartition = Partitioning::Hash(Vec::new(), *batch_size);
            let roundtrip_plan = LogicalPlan::Repartition{input: plan.clone(), partitioning_scheme: no_expr_hrepartition};
            roundtrip_test!(roundtrip_plan);
        }
        Ok(())
    }

/*
    Repartition {
        input: Arc<LogicalPlan>,
        partitioning_scheme: Partitioning,
    }
    CreateExternalTable {
        schema: DFSchemaRef,
        name: String,
        location: String,
        file_type: FileType,
        has_header: bool,
    },
*/

    #[test]
    fn roundtrip_create_external_table()->Result<()>{
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ]);
        use datafusion::logical_plan::{ToDFSchema};
        let df_schema_ref = schema.to_dfschema_ref()?;
        use datafusion::sql::parser::FileType;
        let filetypes:[FileType; 3] = [FileType::NdJson, FileType::Parquet,FileType::CSV];
        for file in filetypes.iter(){
            let create_table_node = LogicalPlan::CreateExternalTable{
                schema: df_schema_ref.clone(),
                name: String::from("TestName"),
                location: String::from("employee.csv"),
                file_type: file.clone(),
                has_header: true,
            };
            roundtrip_test!(create_table_node);
        }

        Ok(())
    }


    #[test]
    fn roundtrip_explain()->Result<()>{
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ]);

        let verbose_plan = LogicalPlanBuilder::scan_csv(
            "employee.csv",
            CsvReadOptions::new().schema(&schema).has_header(true),
            Some(vec![3, 4]),
        ).and_then(|plan| plan.sort(vec![col("salary")]))
        .and_then(|plan| plan.explain(true))
        .and_then(|plan| plan.build()).unwrap();

        let plan = LogicalPlanBuilder::scan_csv(
            "employee.csv",
            CsvReadOptions::new().schema(&schema).has_header(true),
            Some(vec![3, 4]),
        ).and_then(|plan| plan.sort(vec![col("salary")]))
        .and_then(|plan| plan.explain(false))
        .and_then(|plan| plan.build()).unwrap();


        
        roundtrip_test!(plan);
        roundtrip_test!(verbose_plan);
        Ok(())
    }


    #[test]
    fn roundtrip_sort()->Result<()>{
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
            Some(vec![3, 4]),
        ).and_then(|plan| plan.sort(vec![col("salary")]))
        .and_then(|plan| plan.build()).unwrap();
        roundtrip_test!(plan);
        Ok(())
    }


    #[test]
    fn roundtrip_empty_relation()->Result<()>{
        let plan_false = LogicalPlanBuilder::empty(false).build().unwrap();
        roundtrip_test!(plan_false);

        let plan_true = LogicalPlanBuilder::empty(true).build().unwrap();
        roundtrip_test!(plan_true);
        Ok(())
    }

    #[test]
    fn roundtrip_logical_plan() -> Result<()> {
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
            Some(vec![3, 4]),
        )
        .and_then(|plan| plan.aggregate(vec![col("state")], vec![max(col("salary"))]))
        .and_then(|plan| plan.build())
        .unwrap();

        roundtrip_test!(plan);
        Ok(())
    }
}

