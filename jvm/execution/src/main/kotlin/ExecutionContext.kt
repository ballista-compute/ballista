package org.ballistacompute.execution

import org.ballistacompute.datasource.CsvDataSource
import org.ballistacompute.datasource.DataSource
import org.ballistacompute.datatypes.RecordBatch
import org.ballistacompute.logical.*
import org.ballistacompute.optimizer.Optimizer
import org.ballistacompute.planner.QueryPlanner
import org.ballistacompute.sql.SqlParser
import org.ballistacompute.sql.SqlPlanner
import org.ballistacompute.sql.SqlSelect
import org.ballistacompute.sql.SqlTokenizer

/** Execution context */
class ExecutionContext(val batchSize: Int = 1024 * 1024) {

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
    fun csv(filename: String): DataFrame {
        return DataFrameImpl(Scan(filename, CsvDataSource(filename, null, batchSize), listOf()))
    }

    /** Register a DataFrame with the context */
    fun register(tablename: String, df: DataFrame) {
        tables[tablename] = df
    }

    /** Register a CSV data source with the context */
    fun registerDataSource(tablename: String, datasource: DataSource) {
        register(tablename, DataFrameImpl(Scan(tablename, datasource, listOf())))
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
        val optimizedPlan = Optimizer().optimize(plan)
        val physicalPlan = QueryPlanner().createPhysicalPlan(optimizedPlan)
        return physicalPlan.execute()
    }

}