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

import com.univocity.parsers.common.IterableResult
import com.univocity.parsers.common.ParsingContext
import com.univocity.parsers.common.ResultIterator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.*
import org.ballistacompute.datatypes.*
import java.lang.IllegalStateException
import java.util.logging.Logger

import com.univocity.parsers.csv.*
import java.io.*

/**
 * Simple CSV data source. If no schema is provided then it assumes that the first line contains field names and that all values are strings.
 */
class CsvDataSource(val filename: String, val schema: Schema?, private val hasHeaders: Boolean, private val batchSize: Int) : DataSource {

    private val logger = Logger.getLogger(CsvDataSource::class.simpleName)

    private val settings: CsvParserSettings = CsvParserSettings()
    private val parser: CsvParser = CsvParser(settings)

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

        // parser will close once the end of the reader is reached
        parser.beginParsing(file.inputStream().reader())

        if (hasHeaders) {
            // skip first row
            parser.parseNext()
        }

        val projectionIndices = projection.map { name -> finalSchema.fields.indexOfFirst { it.name == name } }
        val readSchema = finalSchema // TODO add this back in .select(projection)
        return ReaderAsSequence(readSchema, projectionIndices, parser, batchSize)
    }

    private fun inferSchema(): Schema {
        logger.fine("inferSchema()")

        val file = File(filename)
        if (!file.exists()) {
            throw FileNotFoundException(file.absolutePath)
        }

        return file.inputStream().use {
            parser.beginParsing(it.reader())

            parser.parseNext()
            val headers = parser.context.parsedHeaders()

            val schema = if(hasHeaders) {
                Schema(headers.map { colName -> Field(colName, ArrowTypes.StringType) })
            } else {
                Schema(headers.mapIndexed { i, _ -> Field("field_$i", ArrowTypes.StringType) } )
            }

            parser.stopParsing()
            schema
        }
    }

}

class ReaderAsSequence(private val schema: Schema,
                       private val projectionIndices: List<Int>,
                       private val parser: CsvParser,
                       private val batchSize: Int) : Sequence<RecordBatch> {
    override fun iterator(): Iterator<RecordBatch> {
        return ReaderIterator(schema, projectionIndices, parser, batchSize)
    }
}

class ReaderIterator(private val schema: Schema,
                     private val projectionIndices: List<Int>,
                     private val parser: CsvParser,
                     private val batchSize: Int) : Iterator<RecordBatch> {

    private val logger = Logger.getLogger(CsvDataSource::class.simpleName)

    private var next: RecordBatch? = null

    override fun hasNext(): Boolean {
        next = nextBatch()

        return next != null
    }

    override fun next(): RecordBatch {
        val out = next

        next = nextBatch()

        return out!!
    }

    private fun nextBatch(): RecordBatch? {
        val rows = ArrayList<List<String>>(batchSize)
        var line = parser.parseNext()


        while(line != null) {
            rows.add(line.asList())

            line = parser.parseNext()
        }

        if(rows.isEmpty()) {
            return null
        }

        return createBatch(rows)
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