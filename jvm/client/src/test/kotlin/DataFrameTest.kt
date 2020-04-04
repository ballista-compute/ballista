package org.ballistacompute.client

import org.ballistacompute.datasource.CsvDataSource
import org.ballistacompute.logical.*
import org.junit.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DataFrameTest {

    @Test
    fun test() {

        val df = DataFrameImpl(Scan("", CsvDataSource("", 0), listOf()))

        val df2 = df
            .filter(col("a") eq lit(123))
            .project(listOf(col("a"), col("b"), col("c")))

        val client = Client("localhost", 50001)
        client.execute(df2.logicalPlan())
    }
}