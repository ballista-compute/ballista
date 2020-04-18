package org.ballistacompute.datasource

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.Float4Vector
import org.apache.arrow.vector.Float8Vector
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.VectorSchemaRoot
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

    private val _schema = schema ?: inferSchema()

    override fun schema(): Schema {
        return _schema
    }

    override fun scan(projection: List<String>): Sequence<RecordBatch> {
        logger.fine("scan() projection=$projection")

        val file = File(filename)
        if (!file.exists()) {
            throw FileNotFoundException(file.absolutePath)
        }
        val b = BufferedReader(FileReader(file))
        val header = b.readLine().split(",")
        val fileColumns = header.map { Field(it, ArrowTypes.StringType) }.toList()

        val projectionIndices = projection.map { name -> fileColumns.indexOfFirst { it.name == name } }

        val schema = when (projectionIndices.size) {
            0 -> Schema(fileColumns)
            else -> Schema(projectionIndices.map { fileColumns[it] })
        }

        return ReaderAsSequence(schema, projectionIndices, b, batchSize)
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
            //TODO null handling
            when (vector) {
                is VarCharVector -> rows.withIndex().forEach { row ->
                    val value = row.value[field.index]
                    vector.set(row.index, value.toByteArray())
                }
                is Float4Vector -> rows.withIndex().forEach { row ->
                    val value = row.value[field.index].toFloat()
                    vector.set(row.index, value)
                }
                is Float8Vector -> rows.withIndex().forEach { row ->
                    val value = row.value[field.index].toDouble()
                    vector.set(row.index, value)
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