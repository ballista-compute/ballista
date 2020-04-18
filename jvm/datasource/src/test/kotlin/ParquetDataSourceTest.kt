package org.ballistacompute.datasource

import org.junit.Test
import org.junit.Ignore
import org.junit.jupiter.api.TestInstance
import java.io.File
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ParquetDataSourceTest {

    val dir = "../testdata"

    @Test
    fun `read parquet schema`() {
        val parquet = ParquetDataSource(File(dir, "alltypes_plain.parquet").absolutePath)
        assertEquals("Schema(fields=[Field(name=id, dataType=Int(32, true)), Field(name=bool_col, dataType=Bool), Field(name=tinyint_col, dataType=Int(32, true)), Field(name=smallint_col, dataType=Int(32, true)), Field(name=int_col, dataType=Int(32, true)), Field(name=bigint_col, dataType=Int(64, true)), Field(name=float_col, dataType=FloatingPoint(SINGLE)), Field(name=double_col, dataType=FloatingPoint(DOUBLE)), Field(name=date_string_col, dataType=Binary), Field(name=string_col, dataType=Binary), Field(name=timestamp_col, dataType=Binary)])", parquet.schema().toString())
    }

    @Test
    @Ignore
    fun `read parquet file`() {
        val parquet = ParquetDataSource(File(dir, "alltypes_plain.parquet").absolutePath)
        val it = parquet.scan(listOf("id")).iterator()
        assertTrue(it.hasNext())

        val batch = it.next()
        assertEquals(1, batch.schema.fields.size)
        assertEquals(8, batch.field(0).size())

        val id = batch.field(0)
        val values = (0..id.size()).map {
            id.getValue(it) ?: "null"
        }
        assertEquals("4,5,6,7,2,3,0,1,null", values.joinToString(","))

        assertFalse(it.hasNext())
    }
}

