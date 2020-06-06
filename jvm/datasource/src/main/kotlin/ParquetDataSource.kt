// Copyright 2020 Andy Grove
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.ballistacompute.datasource

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.arrow.schema.SchemaConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.ballistacompute.datatypes.ArrowFieldVector
import org.ballistacompute.datatypes.Field
import org.ballistacompute.datatypes.RecordBatch
import org.ballistacompute.datatypes.Schema

class ParquetDataSource(private val filename: String) : DataSource {

    override fun schema(): Schema {
        return ParquetScan(filename, listOf()).use {
            val arrowSchema = SchemaConverter().fromParquet(it.schema).arrowSchema
            org.ballistacompute.datatypes.SchemaConverter.fromArrow(arrowSchema)
        }
    }

    override fun scan(projection: List<String>): Sequence<RecordBatch> {
        return ParquetScan(filename, projection)
    }

}

/**
 * Based on blog post at https://www.arm64.ca/post/reading-parquet-files-java/
 */
class ParquetScan(filename: String, private val columns: List<String>) : AutoCloseable, Sequence<RecordBatch> {

    private val reader = ParquetFileReader.open(HadoopInputFile.fromPath(Path(filename), Configuration()))
    val schema = reader.footer.fileMetaData.schema

    override fun iterator(): Iterator<RecordBatch> {
        return ParquetIterator(reader, columns)
    }

    override fun close() {
        reader.close()
    }
}

class ParquetIterator(private val reader: ParquetFileReader, private val projectedColumns: List<String>) : Iterator<RecordBatch> {

    val schema = reader.footer.fileMetaData.schema

    val arrowSchema = SchemaConverter().fromParquet(schema).arrowSchema

    val projectedArrowSchema = org.apache.arrow.vector.types.pojo.Schema(projectedColumns.map { name -> arrowSchema.fields.find { it.name == name } })

    var batch: RecordBatch? = null

    override fun hasNext(): Boolean {
        batch = nextBatch()
        return batch != null
    }

    override fun next(): RecordBatch {
        val next = batch
        batch = null
        return next!!
    }

    private fun nextBatch() : RecordBatch? {
        val pages = reader.readNextRowGroup()
        if (pages == null) {
            return null
        }

        if (pages.rowCount > Integer.MAX_VALUE) {
            throw IllegalStateException()
        }

        val rows = pages.rowCount.toInt()
        println("Reading $rows rows")

        val root = VectorSchemaRoot.create(projectedArrowSchema, RootAllocator(Long.MAX_VALUE))
        root.allocateNew()
        root.rowCount = rows

        val ballistaSchema = org.ballistacompute.datatypes.SchemaConverter.fromArrow(projectedArrowSchema)

        batch = RecordBatch(ballistaSchema, root.fieldVectors.map { ArrowFieldVector(it) })

        //TODO we really want to read directly as columns not rows
//        val columnIO = ColumnIOFactory().getColumnIO(schema)
//        val recordReader: RecordReader<Group> = columnIO.getRecordReader(pages, GroupRecordConverter(schema))
        for (rowIndex in 0 until rows) {
//            val group: Group = recordReader.read()
            for (projectionIndex in 0 until projectedColumns.size) {
//                val primitiveTypeName = projectedArrowSchema.fields[fieldIndex].type.primitiveTypeName
//                println("column $fieldIndex : $primitiveTypeName")
//
//                if (group.getFieldRepetitionCount(fieldIndex) == 1) {
//                    when (primitiveTypeName) {
//                        PrimitiveType.PrimitiveTypeName.INT32 -> {
//                            (root.fieldVectors[projectionIndex] as IntVector).set(rowIndex, group.getInteger(fieldIndex, 0))
//                        }
//                        else -> println("unsupported type $primitiveTypeName")
//                    }
//                }

            }
        }

        return batch
    }


}
