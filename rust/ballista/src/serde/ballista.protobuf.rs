///////////////////////////////////////////////////////////////////////////////////////////////////
// Ballista Logical Plan
///////////////////////////////////////////////////////////////////////////////////////////////////

/// logical expressions
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LogicalExprNode {
    #[prost(oneof="logical_expr_node::ExprType", tags="10, 14, 15, 40, 50, 60, 61, 62, 70, 71, 72, 73, 74, 75, 76")]
    pub expr_type: ::std::option::Option<logical_expr_node::ExprType>,
}
pub mod logical_expr_node {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum ExprType {
        /// column references
        #[prost(string, tag="10")]
        ColumnName(std::string::String),
        /// alias
        #[prost(message, tag="14")]
        Alias(Box<super::AliasNode>),
        #[prost(message, tag="15")]
        Literal(super::ScalarValue),
        /// binary expressions
        #[prost(message, tag="40")]
        BinaryExpr(Box<super::BinaryExprNode>),
        /// aggregate expressions
        #[prost(message, tag="50")]
        AggregateExpr(Box<super::AggregateExprNode>),
        /// null checks
        #[prost(message, tag="60")]
        IsNullExpr(Box<super::IsNull>),
        #[prost(message, tag="61")]
        IsNotNullExpr(Box<super::IsNotNull>),
        #[prost(message, tag="62")]
        NotExpr(Box<super::Not>),
        #[prost(message, tag="70")]
        Between(Box<super::BetweenNode>),
        #[prost(message, tag="71")]
        Case(Box<super::CaseNode>),
        #[prost(message, tag="72")]
        Cast(Box<super::CastNode>),
        #[prost(message, tag="73")]
        Sort(Box<super::SortExprNode>),
        #[prost(message, tag="74")]
        Negative(Box<super::NegativeNode>),
        #[prost(message, tag="75")]
        InList(Box<super::InListNode>),
        #[prost(bool, tag="76")]
        Wildcard(bool),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IsNull {
    #[prost(message, optional, boxed, tag="1")]
    pub expr: ::std::option::Option<::std::boxed::Box<LogicalExprNode>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IsNotNull {
    #[prost(message, optional, boxed, tag="1")]
    pub expr: ::std::option::Option<::std::boxed::Box<LogicalExprNode>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Not {
    #[prost(message, optional, boxed, tag="1")]
    pub expr: ::std::option::Option<::std::boxed::Box<LogicalExprNode>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AliasNode {
    #[prost(message, optional, boxed, tag="1")]
    pub expr: ::std::option::Option<::std::boxed::Box<LogicalExprNode>>,
    #[prost(string, tag="2")]
    pub alias: std::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BinaryExprNode {
    #[prost(message, optional, boxed, tag="1")]
    pub l: ::std::option::Option<::std::boxed::Box<LogicalExprNode>>,
    #[prost(message, optional, boxed, tag="2")]
    pub r: ::std::option::Option<::std::boxed::Box<LogicalExprNode>>,
    #[prost(string, tag="3")]
    pub op: std::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NegativeNode {
    #[prost(message, optional, boxed, tag="1")]
    pub expr: ::std::option::Option<::std::boxed::Box<LogicalExprNode>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InListNode {
    #[prost(message, optional, boxed, tag="1")]
    pub expr: ::std::option::Option<::std::boxed::Box<LogicalExprNode>>,
    #[prost(message, repeated, tag="2")]
    pub list: ::std::vec::Vec<LogicalExprNode>,
    #[prost(bool, tag="3")]
    pub negated: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AggregateExprNode {
    #[prost(enumeration="AggregateFunction", tag="1")]
    pub aggr_function: i32,
    #[prost(message, optional, boxed, tag="2")]
    pub expr: ::std::option::Option<::std::boxed::Box<LogicalExprNode>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BetweenNode {
    #[prost(message, optional, boxed, tag="1")]
    pub expr: ::std::option::Option<::std::boxed::Box<LogicalExprNode>>,
    #[prost(bool, tag="2")]
    pub negated: bool,
    #[prost(message, optional, boxed, tag="3")]
    pub low: ::std::option::Option<::std::boxed::Box<LogicalExprNode>>,
    #[prost(message, optional, boxed, tag="4")]
    pub high: ::std::option::Option<::std::boxed::Box<LogicalExprNode>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CaseNode {
    #[prost(message, optional, boxed, tag="1")]
    pub expr: ::std::option::Option<::std::boxed::Box<LogicalExprNode>>,
    #[prost(message, repeated, tag="2")]
    pub when_then_expr: ::std::vec::Vec<WhenThen>,
    #[prost(message, optional, boxed, tag="3")]
    pub else_expr: ::std::option::Option<::std::boxed::Box<LogicalExprNode>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WhenThen {
    #[prost(message, optional, tag="1")]
    pub when_expr: ::std::option::Option<LogicalExprNode>,
    #[prost(message, optional, tag="2")]
    pub then_expr: ::std::option::Option<LogicalExprNode>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CastNode {
    #[prost(message, optional, boxed, tag="1")]
    pub expr: ::std::option::Option<::std::boxed::Box<LogicalExprNode>>,
    #[prost(message, optional, tag="2")]
    pub arrow_type: ::std::option::Option<ArrowType>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SortExprNode {
    #[prost(message, optional, boxed, tag="1")]
    pub expr: ::std::option::Option<::std::boxed::Box<LogicalExprNode>>,
    #[prost(bool, tag="2")]
    pub asc: bool,
    #[prost(bool, tag="3")]
    pub nulls_first: bool,
}
/// LogicalPlan is a nested type
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LogicalPlanNode {
    #[prost(message, optional, boxed, tag="1")]
    pub input: ::std::option::Option<::std::boxed::Box<LogicalPlanNode>>,
    #[prost(message, optional, tag="10")]
    pub csv_scan: ::std::option::Option<CsvTableScanNode>,
    #[prost(message, optional, tag="11")]
    pub parquet_scan: ::std::option::Option<ParquetTableScanNode>,
    #[prost(message, optional, tag="20")]
    pub projection: ::std::option::Option<ProjectionNode>,
    #[prost(message, optional, tag="21")]
    pub selection: ::std::option::Option<SelectionNode>,
    #[prost(message, optional, tag="22")]
    pub limit: ::std::option::Option<LimitNode>,
    #[prost(message, optional, tag="23")]
    pub aggregate: ::std::option::Option<AggregateNode>,
    #[prost(message, optional, boxed, tag="24")]
    pub join: ::std::option::Option<::std::boxed::Box<JoinNode>>,
    #[prost(message, optional, tag="25")]
    pub sort: ::std::option::Option<SortNode>,
    #[prost(message, optional, tag="26")]
    pub repartition: ::std::option::Option<RepartitionNode>,
    #[prost(message, optional, tag="27")]
    pub empty_relation: ::std::option::Option<EmptyRelationNode>,
    #[prost(message, optional, tag="28")]
    pub create_external_table: ::std::option::Option<CreateExternalTableNode>,
    #[prost(message, optional, tag="29")]
    pub explain: ::std::option::Option<ExplainNode>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProjectionColumns {
    #[prost(string, repeated, tag="1")]
    pub columns: ::std::vec::Vec<std::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CsvTableScanNode {
    #[prost(string, tag="1")]
    pub table_name: std::string::String,
    #[prost(string, tag="2")]
    pub path: std::string::String,
    #[prost(bool, tag="3")]
    pub has_header: bool,
    #[prost(string, tag="4")]
    pub delimiter: std::string::String,
    #[prost(string, tag="5")]
    pub file_extension: std::string::String,
    #[prost(message, optional, tag="6")]
    pub projection: ::std::option::Option<ProjectionColumns>,
    #[prost(message, optional, tag="7")]
    pub schema: ::std::option::Option<Schema>,
    #[prost(message, repeated, tag="8")]
    pub filters: ::std::vec::Vec<LogicalExprNode>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ParquetTableScanNode {
    #[prost(string, tag="1")]
    pub table_name: std::string::String,
    #[prost(string, tag="2")]
    pub path: std::string::String,
    #[prost(message, optional, tag="3")]
    pub projection: ::std::option::Option<ProjectionColumns>,
    #[prost(message, optional, tag="4")]
    pub schema: ::std::option::Option<Schema>,
    #[prost(message, repeated, tag="5")]
    pub filters: ::std::vec::Vec<LogicalExprNode>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProjectionNode {
    #[prost(message, repeated, tag="1")]
    pub expr: ::std::vec::Vec<LogicalExprNode>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SelectionNode {
    #[prost(message, optional, tag="2")]
    pub expr: ::std::option::Option<LogicalExprNode>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SortNode {
    #[prost(message, repeated, tag="1")]
    pub expr: ::std::vec::Vec<LogicalExprNode>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RepartitionNode {
    #[prost(oneof="repartition_node::PartitionMethod", tags="1, 2")]
    pub partition_method: ::std::option::Option<repartition_node::PartitionMethod>,
}
pub mod repartition_node {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum PartitionMethod {
        #[prost(uint64, tag="1")]
        RoundRobin(u64),
        #[prost(message, tag="2")]
        Hash(super::HashRepartition),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HashRepartition {
    #[prost(message, repeated, tag="2")]
    pub hash_expr: ::std::vec::Vec<LogicalExprNode>,
    #[prost(uint64, tag="1")]
    pub batch_size: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EmptyRelationNode {
    #[prost(bool, tag="1")]
    pub produce_one_row: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateExternalTableNode {
    #[prost(string, tag="1")]
    pub name: std::string::String,
    #[prost(string, tag="2")]
    pub location: std::string::String,
    #[prost(enumeration="FileType", tag="3")]
    pub file_type: i32,
    #[prost(bool, tag="4")]
    pub has_header: bool,
    #[prost(message, optional, tag="5")]
    pub schema: ::std::option::Option<Schema>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExplainNode {
    #[prost(bool, tag="1")]
    pub verbose: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DfField {
    #[prost(string, tag="2")]
    pub qualifier: std::string::String,
    #[prost(message, optional, tag="1")]
    pub field: ::std::option::Option<Field>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AggregateNode {
    #[prost(message, repeated, tag="1")]
    pub group_expr: ::std::vec::Vec<LogicalExprNode>,
    #[prost(message, repeated, tag="2")]
    pub aggr_expr: ::std::vec::Vec<LogicalExprNode>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct JoinNode {
    #[prost(message, optional, boxed, tag="1")]
    pub left: ::std::option::Option<::std::boxed::Box<LogicalPlanNode>>,
    #[prost(message, optional, boxed, tag="2")]
    pub right: ::std::option::Option<::std::boxed::Box<LogicalPlanNode>>,
    #[prost(enumeration="JoinType", tag="3")]
    pub join_type: i32,
    #[prost(string, repeated, tag="4")]
    pub left_join_column: ::std::vec::Vec<std::string::String>,
    #[prost(string, repeated, tag="5")]
    pub right_join_column: ::std::vec::Vec<std::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LimitNode {
    #[prost(uint32, tag="1")]
    pub limit: u32,
}
///////////////////////////////////////////////////////////////////////////////////////////////////
// Ballista Physical Plan
///////////////////////////////////////////////////////////////////////////////////////////////////

/// PhysicalPlanNode is a nested type
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalPlanNode {
    #[prost(message, optional, boxed, tag="1")]
    pub input: ::std::option::Option<::std::boxed::Box<PhysicalPlanNode>>,
    #[prost(message, optional, tag="10")]
    pub scan: ::std::option::Option<ScanExecNode>,
    #[prost(message, optional, tag="20")]
    pub projection: ::std::option::Option<ProjectionExecNode>,
    #[prost(message, optional, tag="21")]
    pub selection: ::std::option::Option<SelectionExecNode>,
    #[prost(message, optional, tag="22")]
    pub global_limit: ::std::option::Option<GlobalLimitExecNode>,
    #[prost(message, optional, tag="23")]
    pub local_limit: ::std::option::Option<LocalLimitExecNode>,
    #[prost(message, optional, tag="30")]
    pub hash_aggregate: ::std::option::Option<HashAggregateExecNode>,
    #[prost(message, optional, tag="40")]
    pub shuffle_reader: ::std::option::Option<ShuffleReaderExecNode>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScanExecNode {
    #[prost(string, tag="1")]
    pub path: std::string::String,
    #[prost(uint32, repeated, tag="2")]
    pub projection: ::std::vec::Vec<u32>,
    #[prost(message, optional, tag="3")]
    pub schema: ::std::option::Option<Schema>,
    /// parquet or csv
    #[prost(string, tag="4")]
    pub file_format: std::string::String,
    /// csv specific
    #[prost(bool, tag="5")]
    pub has_header: bool,
    #[prost(uint32, tag="6")]
    pub batch_size: u32,
    /// partition filenames
    #[prost(string, repeated, tag="8")]
    pub filename: ::std::vec::Vec<std::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProjectionExecNode {
    #[prost(message, repeated, tag="1")]
    pub expr: ::std::vec::Vec<LogicalExprNode>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SelectionExecNode {
    #[prost(message, optional, tag="2")]
    pub expr: ::std::option::Option<LogicalExprNode>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HashAggregateExecNode {
    #[prost(message, repeated, tag="1")]
    pub group_expr: ::std::vec::Vec<LogicalExprNode>,
    #[prost(message, repeated, tag="2")]
    pub aggr_expr: ::std::vec::Vec<LogicalExprNode>,
    #[prost(enumeration="AggregateMode", tag="3")]
    pub mode: i32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShuffleReaderExecNode {
    #[prost(message, repeated, tag="1")]
    pub shuffle_id: ::std::vec::Vec<ShuffleId>,
    #[prost(message, optional, tag="2")]
    pub schema: ::std::option::Option<Schema>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GlobalLimitExecNode {
    #[prost(uint32, tag="1")]
    pub limit: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LocalLimitExecNode {
    #[prost(uint32, tag="1")]
    pub limit: u32,
}
///////////////////////////////////////////////////////////////////////////////////////////////////
// Ballista Scheduling
///////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KeyValuePair {
    #[prost(string, tag="1")]
    pub key: std::string::String,
    #[prost(string, tag="2")]
    pub value: std::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Action {
    /// interactive query
    #[prost(message, optional, tag="1")]
    pub query: ::std::option::Option<LogicalPlanNode>,
    /// Execute query and store resulting shuffle partition in memory
    #[prost(message, optional, tag="2")]
    pub task: ::std::option::Option<Task>,
    /// Fetch a shuffle partition from an executor
    #[prost(message, optional, tag="3")]
    pub fetch_shuffle: ::std::option::Option<ShuffleId>,
    /// configuration settings
    #[prost(message, repeated, tag="100")]
    pub settings: ::std::vec::Vec<KeyValuePair>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Task {
    #[prost(string, tag="1")]
    pub job_uuid: std::string::String,
    #[prost(uint32, tag="2")]
    pub stage_id: u32,
    #[prost(uint32, tag="3")]
    pub task_id: u32,
    #[prost(uint32, tag="4")]
    pub partition_id: u32,
    #[prost(message, optional, tag="5")]
    pub plan: ::std::option::Option<PhysicalPlanNode>,
    /// The task could need to read shuffle output from another task
    #[prost(message, repeated, tag="6")]
    pub shuffle_loc: ::std::vec::Vec<ShuffleLocation>,
}
/// Mapping from shuffle id to executor id
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShuffleLocation {
    #[prost(string, tag="1")]
    pub job_uuid: std::string::String,
    #[prost(uint32, tag="2")]
    pub stage_id: u32,
    #[prost(uint32, tag="4")]
    pub partition_id: u32,
    #[prost(string, tag="5")]
    pub executor_id: std::string::String,
    #[prost(string, tag="6")]
    pub executor_host: std::string::String,
    #[prost(uint32, tag="7")]
    pub executor_port: u32,
}
/// Mapping from shuffle id to executor id
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShuffleId {
    #[prost(string, tag="1")]
    pub job_uuid: std::string::String,
    #[prost(uint32, tag="2")]
    pub stage_id: u32,
    #[prost(uint32, tag="4")]
    pub partition_id: u32,
}
///////////////////////////////////////////////////////////////////////////////////////////////////
// Arrow Data Types
///////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Schema {
    #[prost(message, repeated, tag="1")]
    pub columns: ::std::vec::Vec<Field>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Field {
    /// name of the field
    #[prost(string, tag="1")]
    pub name: std::string::String,
    #[prost(message, optional, boxed, tag="2")]
    pub arrow_type: ::std::option::Option<::std::boxed::Box<ArrowType>>,
    #[prost(bool, tag="3")]
    pub nullable: bool,
    /// for complex data types like structs, unions
    #[prost(message, repeated, tag="4")]
    pub children: ::std::vec::Vec<Field>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FixedSizeBinary {
    #[prost(int32, tag="1")]
    pub length: i32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Timestamp {
    #[prost(enumeration="TimeUnit", tag="1")]
    pub time_unit: i32,
    #[prost(string, tag="2")]
    pub timezone: std::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Decimal {
    #[prost(uint64, tag="1")]
    pub whole: u64,
    #[prost(uint64, tag="2")]
    pub fractional: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct List {
    #[prost(message, optional, boxed, tag="1")]
    pub field_type: ::std::option::Option<::std::boxed::Box<Field>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FixedSizeList {
    #[prost(message, optional, boxed, tag="1")]
    pub field_type: ::std::option::Option<::std::boxed::Box<Field>>,
    #[prost(int32, tag="2")]
    pub list_size: i32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Dictionary {
    #[prost(message, optional, boxed, tag="1")]
    pub key: ::std::option::Option<::std::boxed::Box<ArrowType>>,
    #[prost(message, optional, boxed, tag="2")]
    pub value: ::std::option::Option<::std::boxed::Box<ArrowType>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Struct {
    #[prost(message, repeated, tag="1")]
    pub sub_field_types: ::std::vec::Vec<Field>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Union {
    #[prost(message, repeated, tag="1")]
    pub union_types: ::std::vec::Vec<Field>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScalarListValue {
    #[prost(message, optional, tag="1")]
    pub datatype: ::std::option::Option<ScalarType>,
    #[prost(message, repeated, tag="2")]
    pub values: ::std::vec::Vec<ScalarValue>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScalarValue {
    #[prost(oneof="scalar_value::Value", tags="35, 20, 40, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 32, 33, 34, 37, 39, 31")]
    pub value: ::std::option::Option<scalar_value::Value>,
}
pub mod scalar_value {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Value {
        #[prost(bool, tag="35")]
        BoolValue(bool),
        #[prost(string, tag="20")]
        Utf8Value(std::string::String),
        #[prost(string, tag="40")]
        LargeUtf8Value(std::string::String),
        #[prost(int32, tag="21")]
        Int8Value(i32),
        #[prost(int32, tag="22")]
        Int16Value(i32),
        #[prost(int32, tag="23")]
        Int32Value(i32),
        #[prost(int64, tag="24")]
        Int64Value(i64),
        #[prost(uint32, tag="25")]
        Uint8Value(u32),
        #[prost(uint32, tag="26")]
        Uint16Value(u32),
        #[prost(uint32, tag="27")]
        Uint32Value(u32),
        #[prost(uint64, tag="28")]
        Uint64Value(u64),
        #[prost(float, tag="29")]
        Float32Value(f32),
        #[prost(double, tag="30")]
        Float64Value(f64),
        ///Literal Date32 value always has a unit of day
        #[prost(int32, tag="32")]
        Date32Value(i32),
        #[prost(int64, tag="33")]
        TimeMicrosecondValue(i64),
        #[prost(int64, tag="34")]
        TimeNanosecondValue(i64),
        #[prost(message, tag="37")]
        ListValue(super::ScalarListValue),
        #[prost(message, tag="39")]
        NullListValue(super::ScalarType),
        #[prost(enumeration="super::PrimitiveScalarType", tag="31")]
        NullValue(i32),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScalarType {
    #[prost(oneof="scalar_type::Datatype", tags="1, 2")]
    pub datatype: ::std::option::Option<scalar_type::Datatype>,
}
pub mod scalar_type {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Datatype {
        #[prost(enumeration="super::PrimitiveScalarType", tag="1")]
        Scalar(i32),
        #[prost(message, tag="2")]
        List(super::ScalarListType),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScalarListType {
    #[prost(uint64, tag="1")]
    pub depth: u64,
    #[prost(string, repeated, tag="3")]
    pub field_names: ::std::vec::Vec<std::string::String>,
    #[prost(enumeration="PrimitiveScalarType", tag="2")]
    pub deepest_type: i32,
}
/// Broke out into multiple message types so that type 
/// metadata did not need to be in separate message
///All types that are of the empty message types contain no additional metadata
/// about the type
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ArrowType {
    #[prost(oneof="arrow_type::ArrowTypeEnum", tags="1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 32, 15, 16, 31, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30")]
    pub arrow_type_enum: ::std::option::Option<arrow_type::ArrowTypeEnum>,
}
pub mod arrow_type {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum ArrowTypeEnum {
        /// arrow::Type::NA
        #[prost(message, tag="1")]
        None(super::EmptyMessage),
        /// arrow::Type::BOOL
        #[prost(message, tag="2")]
        Bool(super::EmptyMessage),
        /// arrow::Type::UINT8
        #[prost(message, tag="3")]
        Uint8(super::EmptyMessage),
        /// arrow::Type::INT8
        #[prost(message, tag="4")]
        Int8(super::EmptyMessage),
        /// represents arrow::Type fields in src/arrow/type.h
        #[prost(message, tag="5")]
        Uint16(super::EmptyMessage),
        #[prost(message, tag="6")]
        Int16(super::EmptyMessage),
        #[prost(message, tag="7")]
        Uint32(super::EmptyMessage),
        #[prost(message, tag="8")]
        Int32(super::EmptyMessage),
        #[prost(message, tag="9")]
        Uint64(super::EmptyMessage),
        #[prost(message, tag="10")]
        Int64(super::EmptyMessage),
        #[prost(message, tag="11")]
        Float16(super::EmptyMessage),
        #[prost(message, tag="12")]
        Float32(super::EmptyMessage),
        #[prost(message, tag="13")]
        Float64(super::EmptyMessage),
        #[prost(message, tag="14")]
        Utf8(super::EmptyMessage),
        #[prost(message, tag="32")]
        LargeUtf8(super::EmptyMessage),
        #[prost(message, tag="15")]
        Binary(super::EmptyMessage),
        #[prost(int32, tag="16")]
        FixedSizeBinary(i32),
        #[prost(message, tag="31")]
        LargeBinary(super::EmptyMessage),
        #[prost(enumeration="super::DateUnit", tag="17")]
        Date32(i32),
        #[prost(enumeration="super::DateUnit", tag="18")]
        Date64(i32),
        #[prost(enumeration="super::TimeUnit", tag="19")]
        Duration(i32),
        #[prost(message, tag="20")]
        Timestamp(super::Timestamp),
        #[prost(enumeration="super::TimeUnit", tag="21")]
        Time32(i32),
        #[prost(enumeration="super::TimeUnit", tag="22")]
        Time64(i32),
        #[prost(enumeration="super::IntervalUnit", tag="23")]
        Interval(i32),
        #[prost(message, tag="24")]
        Decimal(super::Decimal),
        #[prost(message, tag="25")]
        List(Box<super::List>),
        #[prost(message, tag="26")]
        LargeList(Box<super::List>),
        #[prost(message, tag="27")]
        FixedSizeList(Box<super::FixedSizeList>),
        #[prost(message, tag="28")]
        Struct(super::Struct),
        #[prost(message, tag="29")]
        Union(super::Union),
        #[prost(message, tag="30")]
        Dictionary(Box<super::Dictionary>),
    }
}
///Useful for representing an empty enum variant in rust
/// E.G. enum example{One, Two(i32)}
/// maps to 
/// message example{
///    oneof{
///        EmptyMessage One = 1;
///        i32 Two = 2;
///   }
///}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EmptyMessage {
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum AggregateFunction {
    Min = 0,
    Max = 1,
    Sum = 2,
    Avg = 3,
    Count = 4,
    CountDistinct = 5,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum FileType {
    NdJson = 0,
    Parquet = 1,
    Csv = 2,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum JoinType {
    Inner = 0,
    Left = 1,
    Right = 2,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum AggregateMode {
    Partial = 0,
    Final = 1,
    Complete = 2,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum DateUnit {
    Day = 0,
    DateMillisecond = 1,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum TimeUnit {
    Second = 0,
    TimeMillisecond = 1,
    Microsecond = 2,
    Nanosecond = 3,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum IntervalUnit {
    YearMonth = 0,
    DayTime = 1,
}
/// Contains all valid datafusion scalar type except for 
/// List
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum PrimitiveScalarType {
    /// arrow::Type::BOOL
    Bool = 0,
    /// arrow::Type::UINT8
    Uint8 = 1,
    /// arrow::Type::INT8
    Int8 = 2,
    /// represents arrow::Type fields in src/arrow/type.h
    Uint16 = 3,
    Int16 = 4,
    Uint32 = 5,
    Int32 = 6,
    Uint64 = 7,
    Int64 = 8,
    Float32 = 9,
    Float64 = 10,
    Utf8 = 11,
    LargeUtf8 = 12,
    Date32 = 13,
    TimeMicrosecond = 14,
    TimeNanosecond = 15,
    Null = 16,
}
