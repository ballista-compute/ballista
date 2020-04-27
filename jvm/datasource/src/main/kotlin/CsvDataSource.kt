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
 *
 * Note that this implementation loads the entire CSV file into memory so is not scalable. I plan on implementing
 * a streaming version later on.
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
        val b = BufferedReader(FileReader(file))
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
        val schema = Schema(header.map { Field(it, ArrowTypes.StringType) })
        return schema
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
        var list = mutableListOf<List<String>>()
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

    private fun parseLine(line: String, projection: List<Int>) : List<String> {
        if (projection.isEmpty()) {
            return line.split(",")
        } else {
            //TODO this could be implemented more efficiently
            val splitLine = line.split(",")
            return projection.map { splitLine[it] }.toList()
        }
    }

    private fun createBatch(rows: List<List<String>>) : RecordBatch {
        logger.fine("createBatch() rows=$rows")

        val root = VectorSchemaRoot.create(schema.toArrow(), RootAllocator(Long.MAX_VALUE))
        root.rowCount = rows.size
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

        val batch = RecordBatch(schema, root.fieldVectors.map { ArrowFieldVector(it) })

        logger.fine("Created batch:\n${batch.toCSV()}")

        return batch
    }
}