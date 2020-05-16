// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

///! This file was forked from Apache Arrow.
use std::fmt;
use std::sync::Arc;

use crate::arrow::datatypes::{DataType, Field, Schema};

use crate::arrow::record_batch::RecordBatch;
use crate::datafusion::datasource::MemTable;
use crate::datafusion::error::{ExecutionError, Result};
use crate::datafusion::execution::context::ExecutionContext;
use crate::datafusion::logicalplan::{
    Expr as DFExpr, LogicalPlan as DFLogicalPlan, Operator as DFOperator,
    ScalarValue as DFScalarValue,
};

/// The LogicalPlan represents different types of relations (such as Projection,
/// Selection, etc) and can be created by the SQL query planner and the DataFrame API.
#[derive(Clone)]
pub enum LogicalPlan {
    /// A Projection (essentially a SELECT with an expression list)
    Projection {
        /// The list of expressions
        expr: Vec<Expr>,
        /// The incoming logic plan
        input: Box<LogicalPlan>,
        /// The schema description
        schema: Schema,
    },
    /// A Selection (essentially a WHERE clause with a predicate expression)
    Selection {
        /// The expression
        expr: Expr,
        /// The incoming logic plan
        input: Box<LogicalPlan>,
    },
    /// Represents a list of aggregate expressions with optional grouping expressions
    Aggregate {
        /// The incoming logic plan
        input: Box<LogicalPlan>,
        /// Grouping expressions
        group_expr: Vec<Expr>,
        /// Aggregate expressions
        aggr_expr: Vec<Expr>,
        /// The schema description
        schema: Schema,
    },
    /// Represents a list of sort expressions to be applied to a relation
    Sort {
        /// The sort expressions
        expr: Vec<Expr>,
        /// The incoming logic plan
        input: Box<LogicalPlan>,
        /// The schema description
        schema: Schema,
    },
    /// A table scan against a table that has been registered on a context
    FileScan {
        /// The path to the files
        path: String,
        /// File type (csv, parquet)
        file_type: String,
        /// The underlying table schema
        schema: Schema,
        /// Optional column indices to use as a projection
        projection: Option<Vec<usize>>,
        /// The projected schema
        projected_schema: Schema,
    },
    /// An empty relation with an empty schema
    EmptyRelation {
        /// The schema description
        schema: Schema,
    },
    /// Represents the maximum number of records to return
    Limit {
        /// The expression
        expr: Expr,
        /// The logical plan
        input: Box<LogicalPlan>,
        /// The schema description
        schema: Schema,
    },
    MemoryScan(Vec<RecordBatch>),
}

impl LogicalPlan {
    /// Get a reference to the logical plan's schema
    pub fn schema(&self) -> &Schema {
        match self {
            LogicalPlan::EmptyRelation { schema } => &schema,
            LogicalPlan::FileScan {
                projected_schema, ..
            } => &projected_schema,
            LogicalPlan::Projection { schema, .. } => &schema,
            LogicalPlan::Selection { input, .. } => input.schema(),
            LogicalPlan::Aggregate { schema, .. } => &schema,
            LogicalPlan::Sort { schema, .. } => &schema,
            LogicalPlan::Limit { schema, .. } => &schema,
            LogicalPlan::MemoryScan(batches) => (&batches[0]).schema(),
        }
    }
}

impl LogicalPlan {
    fn fmt_with_indent(&self, f: &mut fmt::Formatter, indent: usize) -> fmt::Result {
        if indent > 0 {
            writeln!(f)?;
            for _ in 0..indent {
                write!(f, "  ")?;
            }
        }
        match *self {
            LogicalPlan::EmptyRelation { .. } => write!(f, "EmptyRelation"),
            LogicalPlan::MemoryScan { .. } => write!(f, "MemoryScan"),
            LogicalPlan::FileScan {
                path: ref table_name,
                ref projected_schema,
                ..
            } => write!(
                f,
                "TableScan: '{}'; schema={:?}",
                table_name, projected_schema
            ),
            LogicalPlan::Projection {
                ref expr,
                ref input,
                ..
            } => {
                write!(f, "Projection: ")?;
                for i in 0..expr.len() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", expr[i])?;
                }
                input.fmt_with_indent(f, indent + 1)
            }
            LogicalPlan::Selection {
                ref expr,
                ref input,
                ..
            } => {
                write!(f, "Selection: {:?}", expr)?;
                input.fmt_with_indent(f, indent + 1)
            }
            LogicalPlan::Aggregate {
                ref input,
                ref group_expr,
                ref aggr_expr,
                ..
            } => {
                write!(
                    f,
                    "Aggregate: groupBy=[{:?}], aggr=[{:?}]",
                    group_expr, aggr_expr
                )?;
                input.fmt_with_indent(f, indent + 1)
            }
            LogicalPlan::Sort {
                ref input,
                ref expr,
                ..
            } => {
                write!(f, "Sort: ")?;
                for i in 0..expr.len() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", expr[i])?;
                }
                input.fmt_with_indent(f, indent + 1)
            }
            LogicalPlan::Limit {
                ref input,
                ref expr,
                ..
            } => {
                write!(f, "Limit: {:?}", expr)?;
                input.fmt_with_indent(f, indent + 1)
            }
        }
    }
}

impl fmt::Debug for LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_with_indent(f, 0)
    }
}

/// Builder for logical plans
pub struct LogicalPlanBuilder {
    plan: LogicalPlan,
}

impl LogicalPlanBuilder {
    /// Create a builder from an existing plan
    pub fn from(plan: &LogicalPlan) -> Self {
        Self { plan: plan.clone() }
    }

    /// Create an empty relation
    pub fn empty() -> Self {
        Self::from(&LogicalPlan::EmptyRelation {
            schema: Schema::empty(),
        })
    }

    /// Scan a data source
    pub fn scan_csv(path: &str, schema: &Schema, projection: Option<Vec<usize>>) -> Result<Self> {
        let projected_schema = projection
            .clone()
            .map(|p| Schema::new(p.iter().map(|i| schema.field(*i).clone()).collect()));
        Ok(Self::from(&LogicalPlan::FileScan {
            path: path.to_owned(),
            file_type: "csv".to_owned(),
            schema: schema.clone(),
            projected_schema: projected_schema.or(Some(schema.clone())).unwrap(),
            projection,
        }))
    }

    /// Apply a projection
    pub fn project(&self, expr: Vec<Expr>) -> Result<Self> {
        let input_schema = self.plan.schema();
        let projected_expr = if expr.contains(&Expr::Wildcard) {
            let mut expr_vec = vec![];
            (0..expr.len()).for_each(|i| match &expr[i] {
                Expr::Wildcard => {
                    (0..input_schema.fields().len())
                        .for_each(|i| expr_vec.push(col_index(i).clone()));
                }
                _ => expr_vec.push(expr[i].clone()),
            });
            expr_vec
        } else {
            expr.clone()
        };

        let schema = Schema::new(exprlist_to_fields(&projected_expr, input_schema)?);

        Ok(Self::from(&LogicalPlan::Projection {
            expr: projected_expr,
            input: Box::new(self.plan.clone()),
            schema: schema,
        }))
    }

    /// Apply a filter
    pub fn filter(&self, expr: Expr) -> Result<Self> {
        Ok(Self::from(&LogicalPlan::Selection {
            expr,
            input: Box::new(self.plan.clone()),
        }))
    }

    /// Apply a limit
    pub fn limit(&self, expr: Expr) -> Result<Self> {
        Ok(Self::from(&LogicalPlan::Limit {
            expr,
            input: Box::new(self.plan.clone()),
            schema: self.plan.schema().clone(),
        }))
    }

    /// Apply a sort
    pub fn sort(&self, expr: Vec<Expr>) -> Result<Self> {
        Ok(Self::from(&LogicalPlan::Sort {
            expr,
            input: Box::new(self.plan.clone()),
            schema: self.plan.schema().clone(),
        }))
    }

    /// Apply an aggregate
    pub fn aggregate(&self, group_expr: Vec<Expr>, aggr_expr: Vec<Expr>) -> Result<Self> {
        let mut all_fields: Vec<Expr> = group_expr.clone();
        aggr_expr.iter().for_each(|x| all_fields.push(x.clone()));

        let aggr_schema = Schema::new(exprlist_to_fields(&all_fields, self.plan.schema())?);

        Ok(Self::from(&LogicalPlan::Aggregate {
            input: Box::new(self.plan.clone()),
            group_expr,
            aggr_expr,
            schema: aggr_schema,
        }))
    }

    /// Build the plan
    pub fn build(&self) -> Result<LogicalPlan> {
        Ok(self.plan.clone())
    }
}

/// Relation expression
#[derive(Clone, PartialEq)]
pub enum Expr {
    /// An aliased expression
    Alias(Box<Expr>, String),
    /// index into a value within the row or complex value
    Column(usize),
    /// Reference to column by name
    UnresolvedColumn(String),
    /// literal value
    Literal(ScalarValue),
    /// binary expression e.g. "age > 21"
    BinaryExpr {
        /// Left-hand side of the expression
        left: Box<Expr>,
        /// The comparison operator
        op: Operator,
        /// Right-hand side of the expression
        right: Box<Expr>,
    },
    /// unary NOT
    Not(Box<Expr>),
    /// unary IS NOT NULL
    IsNotNull(Box<Expr>),
    /// unary IS NULL
    IsNull(Box<Expr>),
    /// cast a value to a different type
    Cast {
        /// The expression being cast
        expr: Box<Expr>,
        /// The `DataType` the expression will yield
        data_type: DataType,
    },
    /// sort expression
    Sort {
        /// The expression to sort on
        expr: Box<Expr>,
        /// The direction of the sort
        asc: bool,
    },
    /// scalar function
    ScalarFunction {
        /// Name of the function
        name: String,
        /// List of expressions to feed to the functions as arguments
        args: Vec<Expr>,
        /// The `DataType` the expression will yield
        return_type: DataType,
    },
    /// aggregate function
    AggregateFunction {
        /// Name of the function
        name: String,
        /// List of expressions to feed to the functions as arguments
        args: Vec<Expr>,
        /// The `DataType` the expression will yield
        return_type: DataType,
    },
    /// Wildcard
    Wildcard,
}

impl Expr {
    /// Find the `DataType` for the expression
    pub fn get_type(&self, schema: &Schema) -> Result<DataType> {
        match self {
            Expr::Alias(expr, _) => expr.get_type(schema),
            Expr::Column(n) => Ok(schema.field(*n).data_type().clone()),
            Expr::UnresolvedColumn(name) => Ok(schema.field_with_name(&name)?.data_type().clone()),
            Expr::Literal(l) => Ok(l.get_datatype()),
            Expr::Cast { data_type, .. } => Ok(data_type.clone()),
            Expr::ScalarFunction { return_type, .. } => Ok(return_type.clone()),
            Expr::AggregateFunction { return_type, .. } => Ok(return_type.clone()),
            Expr::Not(_) => Ok(DataType::Boolean),
            Expr::IsNull(_) => Ok(DataType::Boolean),
            Expr::IsNotNull(_) => Ok(DataType::Boolean),
            Expr::BinaryExpr {
                ref left,
                ref right,
                ref op,
            } => match op {
                Operator::Eq | Operator::NotEq => Ok(DataType::Boolean),
                Operator::Lt | Operator::LtEq => Ok(DataType::Boolean),
                Operator::Gt | Operator::GtEq => Ok(DataType::Boolean),
                Operator::And | Operator::Or => Ok(DataType::Boolean),
                _ => {
                    let left_type = left.get_type(schema)?;
                    let right_type = right.get_type(schema)?;
                    get_supertype(&left_type, &right_type)
                }
            },
            Expr::Sort { ref expr, .. } => expr.get_type(schema),
            Expr::Wildcard => Err(ExecutionError::General(
                "Wildcard expressions are not valid in a logical query plan".to_owned(),
            )),
        }
    }

    /// Perform a type cast on the expression value.
    ///
    /// Will `Err` if the type cast cannot be performed.
    pub fn cast_to(&self, cast_to_type: &DataType, schema: &Schema) -> Result<Expr> {
        let this_type = self.get_type(schema)?;
        if this_type == *cast_to_type {
            Ok(self.clone())
        } else if can_coerce_from(cast_to_type, &this_type) {
            Ok(Expr::Cast {
                expr: Box::new(self.clone()),
                data_type: cast_to_type.clone(),
            })
        } else {
            Err(ExecutionError::General(format!(
                "Cannot automatically convert {:?} to {:?}",
                this_type, cast_to_type
            )))
        }
    }

    /// Equal
    pub fn eq(&self, other: &Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(self.clone()),
            op: Operator::Eq,
            right: Box::new(other.clone()),
        }
    }

    /// Not equal
    pub fn not_eq(&self, other: &Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(self.clone()),
            op: Operator::NotEq,
            right: Box::new(other.clone()),
        }
    }

    /// Greater than
    pub fn gt(&self, other: &Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(self.clone()),
            op: Operator::Gt,
            right: Box::new(other.clone()),
        }
    }

    /// Greater than or equal to
    pub fn gt_eq(&self, other: &Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(self.clone()),
            op: Operator::GtEq,
            right: Box::new(other.clone()),
        }
    }

    /// Less than
    pub fn lt(&self, other: &Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(self.clone()),
            op: Operator::Lt,
            right: Box::new(other.clone()),
        }
    }

    /// Less than or equal to
    pub fn lt_eq(&self, other: &Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(self.clone()),
            op: Operator::LtEq,
            right: Box::new(other.clone()),
        }
    }

    /// Not
    pub fn not(&self) -> Expr {
        Expr::Not(Box::new(self.clone()))
    }

    /// Alias
    pub fn alias(&self, name: &str) -> Expr {
        Expr::Alias(Box::new(self.clone()), name.to_owned())
    }
}

/// Create a column expression based on a column index
pub fn col_index(index: usize) -> Expr {
    Expr::Column(index)
}

/// Create a column expression based on a column name
pub fn col(name: &str) -> Expr {
    Expr::UnresolvedColumn(name.to_owned())
}

/// Create a literal string expression
pub fn lit_str(str: &str) -> Expr {
    Expr::Literal(ScalarValue::Utf8(str.to_owned()))
}

/// Create an convenience function representing a unary scalar function
macro_rules! unary_math_expr {
    ($NAME:expr, $FUNC:ident) => {
        #[allow(missing_docs)]
        pub fn $FUNC(e: Expr) -> Expr {
            scalar_function($NAME, vec![e], DataType::Float64)
        }
    };
}

// generate methods for creating the supported unary math expressions
unary_math_expr!("sqrt", sqrt);
unary_math_expr!("sin", sin);
unary_math_expr!("cos", cos);
unary_math_expr!("tan", tan);
unary_math_expr!("asin", asin);
unary_math_expr!("acos", acos);
unary_math_expr!("atan", atan);
unary_math_expr!("floor", floor);
unary_math_expr!("ceil", ceil);
unary_math_expr!("round", round);
unary_math_expr!("trunc", trunc);
unary_math_expr!("abs", abs);
unary_math_expr!("signum", signum);
unary_math_expr!("exp", exp);
unary_math_expr!("log", ln);
unary_math_expr!("log2", log2);
unary_math_expr!("log10", log10);

/// Create an aggregate expression
pub fn aggregate_expr(name: &str, expr: Expr, return_type: DataType) -> Expr {
    Expr::AggregateFunction {
        name: name.to_owned(),
        args: vec![expr],
        return_type,
    }
}

/// Create an aggregate expression
pub fn scalar_function(name: &str, expr: Vec<Expr>, return_type: DataType) -> Expr {
    Expr::ScalarFunction {
        name: name.to_owned(),
        args: expr,
        return_type,
    }
}

impl fmt::Debug for Expr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Expr::Alias(expr, alias) => write!(f, "{:?} AS {}", expr, alias),
            Expr::Column(i) => write!(f, "#{}", i),
            Expr::UnresolvedColumn(name) => write!(f, "#{}", name),
            Expr::Literal(v) => write!(f, "{:?}", v),
            Expr::Cast { expr, data_type } => write!(f, "CAST({:?} AS {:?})", expr, data_type),
            Expr::Not(expr) => write!(f, "NOT {:?}", expr),
            Expr::IsNull(expr) => write!(f, "{:?} IS NULL", expr),
            Expr::IsNotNull(expr) => write!(f, "{:?} IS NOT NULL", expr),
            Expr::BinaryExpr { left, op, right } => write!(f, "{:?} {:?} {:?}", left, op, right),
            Expr::Sort { expr, asc } => {
                if *asc {
                    write!(f, "{:?} ASC", expr)
                } else {
                    write!(f, "{:?} DESC", expr)
                }
            }
            Expr::ScalarFunction { name, ref args, .. } => {
                write!(f, "{}(", name)?;
                for i in 0..args.len() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", args[i])?;
                }

                write!(f, ")")
            }
            Expr::AggregateFunction { name, ref args, .. } => {
                write!(f, "{}(", name)?;
                for i in 0..args.len() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", args[i])?;
                }

                write!(f, ")")
            }
            Expr::Wildcard => write!(f, "*"),
        }
    }
}

/// Operators applied to expressions
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operator {
    /// Expressions are equal
    Eq,
    /// Expressions are not equal
    NotEq,
    /// Left side is smaller than right side
    Lt,
    /// Left side is smaller or equal to right side
    LtEq,
    /// Left side is greater than right side
    Gt,
    /// Left side is greater or equal to right side
    GtEq,
    /// Addition
    Plus,
    /// Subtraction
    Minus,
    /// Multiplication operator, like `*`
    Multiply,
    /// Division operator, like `/`
    Divide,
    /// Remainder operator, like `%`
    Modulus,
    /// Logical AND, like `&&`
    And,
    /// Logical OR, like `||`
    Or,
    /// Logical NOT, like `!`
    Not,
    /// Matches a wildcard pattern
    Like,
    /// Does not match a wildcard pattern
    NotLike,
}

/// ScalarValue enumeration
#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    /// null value
    Null,
    /// true or false value
    Boolean(bool),
    /// 32bit float
    Float32(f32),
    /// 64bit float
    Float64(f64),
    /// signed 8bit int
    Int8(i8),
    /// signed 16bit int
    Int16(i16),
    /// signed 32bit int
    Int32(i32),
    /// signed 64bit int
    Int64(i64),
    /// unsigned 8bit int
    UInt8(u8),
    /// unsigned 16bit int
    UInt16(u16),
    /// unsigned 32bit int
    UInt32(u32),
    /// unsigned 64bit int
    UInt64(u64),
    /// utf-8 encoded string
    Utf8(String),
    /// List of scalars packed as a struct
    Struct(Vec<ScalarValue>),
}

impl ScalarValue {
    /// Getter for the `DataType` of the value
    pub fn get_datatype(&self) -> DataType {
        match *self {
            ScalarValue::Boolean(_) => DataType::Boolean,
            ScalarValue::UInt8(_) => DataType::UInt8,
            ScalarValue::UInt16(_) => DataType::UInt16,
            ScalarValue::UInt32(_) => DataType::UInt32,
            ScalarValue::UInt64(_) => DataType::UInt64,
            ScalarValue::Int8(_) => DataType::Int8,
            ScalarValue::Int16(_) => DataType::Int16,
            ScalarValue::Int32(_) => DataType::Int32,
            ScalarValue::Int64(_) => DataType::Int64,
            ScalarValue::Float32(_) => DataType::Float32,
            ScalarValue::Float64(_) => DataType::Float64,
            ScalarValue::Utf8(_) => DataType::Utf8,
            _ => panic!("Cannot treat {:?} as scalar value", self),
        }
    }
}

/// Verify a given type cast can be performed
pub fn can_coerce_from(type_into: &DataType, type_from: &DataType) -> bool {
    use self::DataType::*;
    match type_into {
        Int8 => match type_from {
            Int8 => true,
            _ => false,
        },
        Int16 => match type_from {
            Int8 | Int16 | UInt8 => true,
            _ => false,
        },
        Int32 => match type_from {
            Int8 | Int16 | Int32 | UInt8 | UInt16 => true,
            _ => false,
        },
        Int64 => match type_from {
            Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 => true,
            _ => false,
        },
        UInt8 => match type_from {
            UInt8 => true,
            _ => false,
        },
        UInt16 => match type_from {
            UInt8 | UInt16 => true,
            _ => false,
        },
        UInt32 => match type_from {
            UInt8 | UInt16 | UInt32 => true,
            _ => false,
        },
        UInt64 => match type_from {
            UInt8 | UInt16 | UInt32 | UInt64 => true,
            _ => false,
        },
        Float32 => match type_from {
            Int8 | Int16 | Int32 | Int64 => true,
            UInt8 | UInt16 | UInt32 | UInt64 => true,
            Float32 => true,
            _ => false,
        },
        Float64 => match type_from {
            Int8 | Int16 | Int32 | Int64 => true,
            UInt8 | UInt16 | UInt32 | UInt64 => true,
            Float32 | Float64 => true,
            _ => false,
        },
        Utf8 => true,
        _ => false,
    }
}

/// Create field meta-data from an expression, for use in a result set schema
pub fn expr_to_field(e: &Expr, input_schema: &Schema) -> Result<Field> {
    match e {
        Expr::Alias(expr, name) => Ok(Field::new(name, expr.get_type(input_schema)?, true)),
        Expr::UnresolvedColumn(name) => Ok(input_schema.field_with_name(&name)?.clone()),
        Expr::Column(i) => {
            let input_schema_field_count = input_schema.fields().len();
            if *i < input_schema_field_count {
                Ok(input_schema.fields()[*i].clone())
            } else {
                Err(ExecutionError::General(format!(
                    "Column index {} out of bounds for input schema with {} field(s)",
                    *i, input_schema_field_count
                )))
            }
        }
        Expr::Literal(ref lit) => Ok(Field::new("lit", lit.get_datatype(), true)),
        Expr::ScalarFunction {
            ref name,
            ref return_type,
            ..
        } => Ok(Field::new(&name, return_type.clone(), true)),
        Expr::AggregateFunction {
            ref name,
            ref return_type,
            ..
        } => Ok(Field::new(&name, return_type.clone(), true)),
        Expr::Cast { ref data_type, .. } => Ok(Field::new("cast", data_type.clone(), true)),
        Expr::BinaryExpr {
            ref left,
            ref right,
            ..
        } => {
            let left_type = left.get_type(input_schema)?;
            let right_type = right.get_type(input_schema)?;
            Ok(Field::new(
                "binary_expr",
                get_supertype(&left_type, &right_type).unwrap(),
                true,
            ))
        }
        _ => Err(ExecutionError::NotImplemented(format!(
            "Cannot determine schema type for expression {:?}",
            e
        ))),
    }
}

/// Create field meta-data from an expression, for use in a result set schema
pub fn exprlist_to_fields(expr: &[Expr], input_schema: &Schema) -> Result<Vec<Field>> {
    expr.iter()
        .map(|e| expr_to_field(e, input_schema))
        .collect()
}

/// Given two datatypes, determine the supertype that both types can safely be cast to
pub fn get_supertype(l: &DataType, r: &DataType) -> Result<DataType> {
    match _get_supertype(l, r) {
        Some(dt) => Ok(dt),
        None => _get_supertype(r, l).ok_or_else(|| {
            ExecutionError::InternalError(format!(
                "Failed to determine supertype of {:?} and {:?}",
                l, r
            ))
        }),
    }
}

/// Given two datatypes, determine the supertype that both types can safely be cast to
fn _get_supertype(l: &DataType, r: &DataType) -> Option<DataType> {
    use crate::arrow::datatypes::DataType::*;
    match (l, r) {
        (UInt8, Int8) => Some(Int8),
        (UInt8, Int16) => Some(Int16),
        (UInt8, Int32) => Some(Int32),
        (UInt8, Int64) => Some(Int64),

        (UInt16, Int16) => Some(Int16),
        (UInt16, Int32) => Some(Int32),
        (UInt16, Int64) => Some(Int64),

        (UInt32, Int32) => Some(Int32),
        (UInt32, Int64) => Some(Int64),

        (UInt64, Int64) => Some(Int64),

        (Int8, UInt8) => Some(Int8),

        (Int16, UInt8) => Some(Int16),
        (Int16, UInt16) => Some(Int16),

        (Int32, UInt8) => Some(Int32),
        (Int32, UInt16) => Some(Int32),
        (Int32, UInt32) => Some(Int32),

        (Int64, UInt8) => Some(Int64),
        (Int64, UInt16) => Some(Int64),
        (Int64, UInt32) => Some(Int64),
        (Int64, UInt64) => Some(Int64),

        (UInt8, UInt8) => Some(UInt8),
        (UInt8, UInt16) => Some(UInt16),
        (UInt8, UInt32) => Some(UInt32),
        (UInt8, UInt64) => Some(UInt64),
        (UInt8, Float32) => Some(Float32),
        (UInt8, Float64) => Some(Float64),

        (UInt16, UInt8) => Some(UInt16),
        (UInt16, UInt16) => Some(UInt16),
        (UInt16, UInt32) => Some(UInt32),
        (UInt16, UInt64) => Some(UInt64),
        (UInt16, Float32) => Some(Float32),
        (UInt16, Float64) => Some(Float64),

        (UInt32, UInt8) => Some(UInt32),
        (UInt32, UInt16) => Some(UInt32),
        (UInt32, UInt32) => Some(UInt32),
        (UInt32, UInt64) => Some(UInt64),
        (UInt32, Float32) => Some(Float32),
        (UInt32, Float64) => Some(Float64),

        (UInt64, UInt8) => Some(UInt64),
        (UInt64, UInt16) => Some(UInt64),
        (UInt64, UInt32) => Some(UInt64),
        (UInt64, UInt64) => Some(UInt64),
        (UInt64, Float32) => Some(Float32),
        (UInt64, Float64) => Some(Float64),

        (Int8, Int8) => Some(Int8),
        (Int8, Int16) => Some(Int16),
        (Int8, Int32) => Some(Int32),
        (Int8, Int64) => Some(Int64),
        (Int8, Float32) => Some(Float32),
        (Int8, Float64) => Some(Float64),

        (Int16, Int8) => Some(Int16),
        (Int16, Int16) => Some(Int16),
        (Int16, Int32) => Some(Int32),
        (Int16, Int64) => Some(Int64),
        (Int16, Float32) => Some(Float32),
        (Int16, Float64) => Some(Float64),

        (Int32, Int8) => Some(Int32),
        (Int32, Int16) => Some(Int32),
        (Int32, Int32) => Some(Int32),
        (Int32, Int64) => Some(Int64),
        (Int32, Float32) => Some(Float32),
        (Int32, Float64) => Some(Float64),

        (Int64, Int8) => Some(Int64),
        (Int64, Int16) => Some(Int64),
        (Int64, Int32) => Some(Int64),
        (Int64, Int64) => Some(Int64),
        (Int64, Float32) => Some(Float32),
        (Int64, Float64) => Some(Float64),

        (Float32, Float32) => Some(Float32),
        (Float32, Float64) => Some(Float64),
        (Float64, Float32) => Some(Float64),
        (Float64, Float64) => Some(Float64),

        (Utf8, _) => Some(Utf8),
        (_, Utf8) => Some(Utf8),

        (Boolean, Boolean) => Some(Boolean),

        _ => None,
    }
}

/// Translate Ballista plan to DataFusion plan
pub fn translate_plan(ctx: &mut ExecutionContext, plan: &LogicalPlan) -> Result<DFLogicalPlan> {
    match plan {
        LogicalPlan::MemoryScan(batches) => {
            let table_name = "df_t0"; //TODO generate unique table name
            let schema = (&batches[0]).schema().as_ref();
            let provider = MemTable::new(Arc::new(schema.clone()), batches.clone())?;
            ctx.register_table(table_name, Box::new(provider));
            Ok(DFLogicalPlan::TableScan {
                schema_name: "default".to_owned(),
                table_name: table_name.to_owned(),
                table_schema: Box::new(schema.clone()),
                projected_schema: Box::new(schema.clone()),
                projection: None,
            })
        }
        LogicalPlan::FileScan {
            path,
            file_type,
            schema,
            projection,
            projected_schema,
        } => {
            //TODO generate unique table name
            let table_name = "tbd".to_owned();

            match file_type.as_str() {
                "csv" => ctx.register_csv(&table_name, path.as_str(), schema, true),
                "parquet" => ctx.register_parquet(&table_name, path.as_str())?,
                _ => unimplemented!(),
            };

            Ok(DFLogicalPlan::TableScan {
                schema_name: "default".to_owned(),
                table_name: table_name.clone(),
                table_schema: Box::new(schema.clone()),
                projected_schema: Box::new(projected_schema.clone()),
                projection: projection.clone(),
            })
        }
        LogicalPlan::Projection {
            expr,
            input,
            schema,
        } => Ok(DFLogicalPlan::Projection {
            expr: expr
                .iter()
                .map(|e| translate_expr(e))
                .collect::<Result<Vec<_>>>()?,
            input: Box::new(translate_plan(ctx, input)?),
            schema: Box::new(schema.clone()),
        }),
        LogicalPlan::Selection { expr, input } => Ok(DFLogicalPlan::Selection {
            expr: translate_expr(expr)?,
            input: Box::new(translate_plan(ctx, input)?),
        }),
        LogicalPlan::Aggregate {
            group_expr,
            aggr_expr,
            input,
            schema,
        } => Ok(DFLogicalPlan::Aggregate {
            group_expr: group_expr
                .iter()
                .map(|e| translate_expr(e))
                .collect::<Result<Vec<_>>>()?,
            aggr_expr: aggr_expr
                .iter()
                .map(|e| translate_expr(e))
                .collect::<Result<Vec<_>>>()?,
            input: Box::new(translate_plan(ctx, input)?),
            schema: Box::new(schema.clone()),
        }),
        LogicalPlan::Limit {
            expr,
            input,
            schema,
        } => Ok(DFLogicalPlan::Limit {
            expr: translate_expr(expr)?,
            input: Box::new(translate_plan(ctx, input)?),
            schema: Box::new(schema.clone()),
        }),
        other => Err(ExecutionError::General(format!(
            "Cannot translate operator to DataFusion: {:?}",
            other
        ))),
    }
}

/// Translate Ballista expression to DataFusion expression
fn translate_expr(expr: &Expr) -> Result<DFExpr> {
    match expr {
        Expr::Alias(expr, alias) => Ok(DFExpr::Alias(
            Box::new(translate_expr(expr.as_ref())?),
            alias.clone(),
        )),
        Expr::Column(index) => Ok(DFExpr::Column(*index)),
        Expr::UnresolvedColumn(name) => Ok(DFExpr::UnresolvedColumn(name.clone())),
        Expr::Literal(value) => {
            let value = translate_scalar_value(value)?;
            Ok(DFExpr::Literal(value.clone()))
        }
        Expr::BinaryExpr { left, op, right } => {
            let left = translate_expr(left)?;
            let right = translate_expr(right)?;
            let op = translate_operator(op)?;
            Ok(DFExpr::BinaryExpr {
                left: Box::new(left),
                op,
                right: Box::new(right),
            })
        }
        Expr::AggregateFunction {
            name,
            args,
            return_type,
        } => {
            let args = args
                .iter()
                .map(|e| translate_expr(e))
                .collect::<Result<Vec<_>>>()?;
            Ok(DFExpr::AggregateFunction {
                name: name.to_owned(),
                args,
                return_type: return_type.clone(),
            })
        }
        other => Err(ExecutionError::General(format!(
            "Cannot translate expression to DataFusion: {:?}",
            other
        ))),
    }
}

fn translate_operator(op: &Operator) -> Result<DFOperator> {
    match op {
        Operator::Eq => Ok(DFOperator::Eq),
        Operator::NotEq => Ok(DFOperator::NotEq),
        Operator::Lt => Ok(DFOperator::Lt),
        Operator::LtEq => Ok(DFOperator::LtEq),
        Operator::Gt => Ok(DFOperator::Gt),
        Operator::GtEq => Ok(DFOperator::GtEq),
        Operator::And => Ok(DFOperator::And),
        Operator::Or => Ok(DFOperator::Or),
        Operator::Plus => Ok(DFOperator::Plus),
        Operator::Minus => Ok(DFOperator::Minus),
        Operator::Multiply => Ok(DFOperator::Multiply),
        Operator::Divide => Ok(DFOperator::Divide),
        Operator::Like => Ok(DFOperator::Like),
        Operator::NotLike => Ok(DFOperator::NotLike),
        Operator::Modulus => Ok(DFOperator::Modulus),
        other => Err(ExecutionError::General(format!(
            "Cannot translate binary operator to DataFusion: {:?}",
            other
        ))),
    }
}

fn translate_scalar_value(value: &ScalarValue) -> Result<DFScalarValue> {
    match value {
        ScalarValue::Boolean(v) => Ok(DFScalarValue::Boolean(*v)),
        ScalarValue::UInt8(v) => Ok(DFScalarValue::UInt8(*v)),
        ScalarValue::UInt16(v) => Ok(DFScalarValue::UInt16(*v)),
        ScalarValue::UInt32(v) => Ok(DFScalarValue::UInt32(*v)),
        ScalarValue::UInt64(v) => Ok(DFScalarValue::UInt64(*v)),
        ScalarValue::Int8(v) => Ok(DFScalarValue::Int8(*v)),
        ScalarValue::Int16(v) => Ok(DFScalarValue::Int16(*v)),
        ScalarValue::Int32(v) => Ok(DFScalarValue::Int32(*v)),
        ScalarValue::Int64(v) => Ok(DFScalarValue::Int64(*v)),
        ScalarValue::Float32(v) => Ok(DFScalarValue::Float32(*v)),
        ScalarValue::Float64(v) => Ok(DFScalarValue::Float64(*v)),
        ScalarValue::Utf8(v) => Ok(DFScalarValue::Utf8(v.clone())),
        other => Err(ExecutionError::General(format!(
            "Cannot translate scalar value to DataFusion: {:?}",
            other
        ))),
    }
}
