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
import org.apache.arrow.vector.*
import org.ballistacompute.datatypes.*
import java.io.BufferedReader
import java.io.File
import java.io.FileNotFoundException
import java.io.FileReader
import java.lang.IllegalStateException
import java.util.logging.Logger

/**
 * Simple CSV data source. If no schema is provided then it assumes that the first line contains field names and that all values are strings.
 */
class CsvDataSource(val filename: String, val schema: Schema?, private val batchSize: Int) : DataSource {

    private val logger = Logger.getLogger(CsvDataSource::class.simpleName)

    private val finalSchema = schema ?: inferSchema()

    override fun schema(): Schema {
        return finalSchema
    }

    override fun scan(projection: List<String>): Sequence<RecordBatch> {
        logger.fine("scan() projection=$projection")

        val file = File(filename)
        if (!file.exists()) {
            throw FileNotFoundException(file.absolutePath)
        }
        val b = BufferedReader(FileReader(file), 16*1024*1024) //TODO configurable buffer size
        b.readLine() // skip header

        val projectionIndices = projection.map { name -> finalSchema.fields.indexOfFirst { it.name == name } }
        val readSchema = finalSchema.select(projection)
        return ReaderAsSequence(readSchema, projectionIndices, b, batchSize)
    }

    private fun inferSchema(): Schema {
        logger.fine("inferSchema()")
        val file = File(filename)
        if (!file.exists()) {
            throw FileNotFoundException(file.absolutePath)
        }
        val b = BufferedReader(FileReader(file))
        val header = b.readLine().split(",")
        return Schema(header.map { Field(it, ArrowTypes.StringType) })
    }

}

class ReaderAsSequence(private val schema: Schema,
                       private val projectionIndices: List<Int>,
                       private val r: BufferedReader,
                       private val batchSize: Int) : Sequence<RecordBatch> {
    override fun iterator(): Iterator<RecordBatch> {
        return ReaderIterator(schema, projectionIndices, r, batchSize)
    }
}

class ReaderIterator(private val schema: Schema,
                     private val projectionIndices: List<Int>,
                     private val r: BufferedReader,
                     private val batchSize: Int) : Iterator<RecordBatch> {

    private val logger = Logger.getLogger(CsvDataSource::class.simpleName)

    private var rows: List<List<String>> = listOf()

    override fun hasNext(): Boolean {
        var list = ArrayList<List<String>>(batchSize)
        var line = r.readLine()
        while (line != null) {
            list.add(parseLine(line, projectionIndices))
            if (list.size == batchSize) {
                break
            }
            line = r.readLine()
        }
        rows = list.toList()
        return rows.size > 0
    }

    override fun next(): RecordBatch {
        return createBatch(rows)
    }

    private val fieldSeparators = mutableListOf<Int>()

    private fun parseLine(line: String, projection: List<Int>) : List<String> {
        if (projection.isEmpty()) {
            return line.split(",")
        } else {
            // find field delimiters
            var i=0
            fieldSeparators.clear()
            fieldSeparators.add(0) // first field starts at zero offset
            while (i<line.length) {
                //TODO handle strings, escaped quotes, etc
                if (line[i] == ',') {
                    fieldSeparators.add(i)
                }
                i++
            }
            return projection.map {
                val startIndex = fieldSeparators[it] + 1
                if (it == fieldSeparators.size) {
                    line.substring(startIndex)
                } else {
                    line.substring(startIndex, fieldSeparators[it+1])
                }
            }.toList()
        }
    }

    private fun createBatch(rows: List<List<String>>) : RecordBatch {
        val root = VectorSchemaRoot.create(schema.toArrow(), RootAllocator(Long.MAX_VALUE))
        root.fieldVectors.forEach {
            it.setInitialCapacity(rows.size)
        }
        root.allocateNew()

        root.fieldVectors.withIndex().forEach { field ->
            val vector = field.value
            when (vector) {
                is VarCharVector -> rows.withIndex().forEach { row ->
                    val valueStr = row.value[field.index].trim()
                    vector.set(row.index, valueStr.toByteArray())
                }
                is TinyIntVector -> rows.withIndex().forEach { row ->
                    val valueStr = row.value[field.index].trim()
                    if (valueStr.isEmpty()) {
                        vector.setNull(row.index)
                    } else {
                        vector.set(row.index, valueStr.toByte())
                    }
                }
                is SmallIntVector -> rows.withIndex().forEach { row ->
                    val valueStr = row.value[field.index].trim()
                    if (valueStr.isEmpty()) {
                        vector.setNull(row.index)
                    } else {
                        vector.set(row.index, valueStr.toShort())
                    }
                }
                is IntVector -> rows.withIndex().forEach { row ->
                    val valueStr = row.value[field.index].trim()
                    if (valueStr.isEmpty()) {
                        vector.setNull(row.index)
                    } else {
                        vector.set(row.index, valueStr.toInt())
                    }
                }
                is BigIntVector -> rows.withIndex().forEach { row ->
                    val valueStr = row.value[field.index].trim()
                    if (valueStr.isEmpty()) {
                        vector.setNull(row.index)
                    } else {
                        vector.set(row.index, valueStr.toLong())
                    }
                }
                is Float4Vector -> rows.withIndex().forEach { row ->
                    val valueStr = row.value[field.index].trim()
                    if (valueStr.isEmpty()) {
                        vector.setNull(row.index)
                    } else {
                        vector.set(row.index, valueStr.toFloat())
                    }
                }
                is Float8Vector -> rows.withIndex().forEach { row ->
                    val valueStr = row.value[field.index].trim()
                    if (valueStr.isEmpty()) {
                        vector.setNull(row.index)
                    } else {
                        vector.set(row.index, valueStr.toDouble())
                    }
                }
                else -> throw IllegalStateException("No support for reading CSV columns with data type $vector")
            }
            field.value.valueCount = rows.size
        }

        return RecordBatch(schema, root.fieldVectors.map { ArrowFieldVector(it) })
    }
}