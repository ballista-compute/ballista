package org.ballistacompute.protobuf.test

import org.ballistacompute.datasource.CsvDataSource
import org.ballistacompute.logical.*
import org.ballistacompute.protobuf.ProtobufDeserializer
import org.ballistacompute.protobuf.ProtobufSerializer
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SerdeTest {

    @Test
    fun `Convert plan to protobuf`() {

        val df = csv()
                .filter(col("state") eq lit("CO"))
                .project(listOf(col("id"), col("first_name"), col("last_name")))

        val logicalPlan = roundtrip(df)

        val expected =
            "Projection: #id, #first_name, #last_name\n" +
                    "\tSelection: #state = 'CO'\n" +
                    "\t\tScan: ../testdata/employee.csv; projection=None\n"

        assertEquals(expected, format(logicalPlan))
    }

    private fun roundtrip(df: DataFrame) : LogicalPlan {
        val protobuf = ProtobufSerializer().toProto(df.logicalPlan())
        return ProtobufDeserializer(mapOf()).fromProto(protobuf)
    }

    private fun csv(): DataFrame {
        val employeeCsv = "../testdata/employee.csv"
        val csv = CsvDataSource(employeeCsv, 1024)
        println(csv.schema())
        return DataFrameImpl(Scan("employee", csv, listOf()))
    }
}