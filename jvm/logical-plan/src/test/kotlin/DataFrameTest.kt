package org.ballistacompute.logical

import org.ballistacompute.datasource.CsvDataSource

import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DataFrameTest {

    @Test
    fun `build DataFrame`() {

        val df = csv()
                .filter(col("state") eq lit("CO"))
                .project(listOf(col("id"), col("first_name"), col("last_name")))

        val expected =
                "Projection: #id, #first_name, #last_name\n" +
                        "\tSelection: #state = 'CO'\n" +
                        "\t\tScan: employee; projection=None\n"

        assertEquals(expected, format(df.logicalPlan()))
    }

    @Test
    fun `multiplier and alias`() {

        val df = csv()
                .filter(col("state") eq lit("CO"))
                .project(listOf(
                        col("id"),
                        col("first_name"),
                        col("last_name"),
                        col("salary"),
                        (col("salary") mult lit(0.1)) alias "bonus"))
                .filter(col("bonus") gt lit(1000))

        val expected =
                "Selection: #bonus > 1000\n" +
                        "\tProjection: #id, #first_name, #last_name, #salary, #salary * 0.1 as bonus\n" +
                        "\t\tSelection: #state = 'CO'\n" +
                        "\t\t\tScan: employee; projection=None\n"

        val actual = format(df.logicalPlan())

        assertEquals(expected, actual)
    }

    @Test
    fun `aggregate query`() {

        val df = csv()
                .aggregate(listOf(col("state")), listOf(Min(col("salary")), Max(col("salary")), Count(col("salary"))))

        assertEquals(
                "Aggregate: groupExpr=[#state], aggregateExpr=[MIN(#salary), MAX(#salary), COUNT(#salary)]\n" +
                        "\tScan: employee; projection=None\n", format(df.logicalPlan()))
    }

    private fun csv() : DataFrame {
        val employeeCsv = "../testdata/employee.csv"
        return DataFrameImpl(Scan("employee", CsvDataSource(employeeCsv, null, 1024), listOf()))
    }
}