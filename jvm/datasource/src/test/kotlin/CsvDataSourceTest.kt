package org.ballistacompute.datasource

import org.junit.Test
import org.junit.jupiter.api.TestInstance
import java.io.File

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CsvDataSourceTest {

    val dir = "../testdata"

    @Test
    fun `read csv`() {
        val csv = CsvDataSource(File(dir, "employee.csv").absolutePath, 1024)
        val result = csv.scan(listOf())
        result.asSequence().forEach {
            val field = it.field(0)
            println(field.size())
        }
    }

}

