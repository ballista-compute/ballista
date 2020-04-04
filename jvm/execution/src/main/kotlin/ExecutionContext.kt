package org.ballistacompute.execution

import org.ballistacompute.datasource.CsvDataSource
import org.ballistacompute.datatypes.RecordBatch
import org.ballistacompute.datatypes.ColumnVector
import org.ballistacompute.datatypes.ArrowFieldVector
import org.ballistacompute.datatypes.LiteralValueVector
import org.ballistacompute.datatypes.ArrowVectorBuilder

import org.ballistacompute.logical.DataFrame
import org.ballistacompute.logical.DataFrameImpl
import org.ballistacompute.logical.LogicalPlan
import org.ballistacompute.logical.Scan
import org.ballistacompute.planner.QueryPlanner
import org.ballistacompute.sql.SqlParser
import org.ballistacompute.sql.SqlPlanner
import org.ballistacompute.sql.SqlSelect
import org.ballistacompute.sql.SqlTokenizer

/** Execution context */
class ExecutionContext {

    /** Tables registered with this context */
    private val tables = mutableMapOf<String, DataFrame>()

    /** Create a DataFrame for the given SQL Select */
    fun sql(sql: String): DataFrame {
        val tokens = SqlTokenizer(sql).tokenize()
        val ast = SqlParser(tokens).parse() as SqlSelect
        val df = SqlPlanner().createDataFrame(ast, tables)
        return DataFrameImpl(df.logicalPlan())
    }

    /** Get a DataFrame representing the specified CSV file */
    fun csv(filename: String, batchSize: Int = 1000): DataFrame {
        return DataFrameImpl(Scan(filename, CsvDataSource(filename, batchSize), listOf()))
    }

    /** Register a DataFrame with the context */
    fun register(tablename: String, df: DataFrame) {
        tables[tablename] = df
    }

    /** Register a CSV data source with the context */
    fun registerCsv(tablename: String, filename: String) {
        register(tablename, csv(filename))
    }
    /** Execute the logical plan represented by a DataFrame */
    fun execute(df: DataFrame) : Sequence<RecordBatch> {
        return execute(df.logicalPlan())
    }

    /** Execute the provided logical plan */
    fun execute(plan: LogicalPlan) : Sequence<RecordBatch> {
        val physicalPlan = QueryPlanner().createPhysicalPlan(plan)
        return physicalPlan.execute()
    }

}