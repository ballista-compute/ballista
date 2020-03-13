package org.ballistacompute.protobuf.test

import org.ballistacompute.datasource.CsvDataSource
import org.ballistacompute.logical.*
import org.ballistacompute.protobuf.ProtoUtils
import org.junit.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SerdeTest {

    @Test
    fun `Convert plan to protobuf`() {

        val df = csv()
                .filter(col("state") eq lit("CO"))
                .select(listOf(col("id"), col("first_name"), col("last_name")))

        val protobuf = ProtoUtils().toProto(df.logicalPlan())

//    val expected =
//            "Projection: #id, #first_name, #last_name\n" +
//                    "\tSelection: #state = 'CO'\n" +
//                    "\t\tScan: employee; projection=None\n"
//
//    assertEquals(expected, format(df.logicalPlan()))
    }

    private fun csv(): DataFrame {
        val employeeCsv = "../testdata/employee.csv"
        return DataFrameImpl(Scan("employee", CsvDataSource(employeeCsv, 1024), listOf()))
    }
}