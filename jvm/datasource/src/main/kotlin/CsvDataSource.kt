package org.ballistacompute.datasource

import org.ballistacompute.datatypes.RecordBatch

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.Schema
import org.ballistacompute.datatypes.ArrowFieldVector
import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import java.util.logging.Logger

/**
 * Simple CSV data source that assumes that the first line contains field names and that all values are strings.
 *
 * Note that this implementation loads the entire CSV file into memory so is not scalable. I plan on implementing
 * a streaming version later on.
 */
class CsvDataSource(private val filename: String, private val batchSize: Int) : DataSource {

    private val logger = Logger.getLogger(CsvDataSource::class.simpleName)

    override fun schema(): Schema {
        logger.fine("schema()")
        val b = BufferedReader(FileReader(filename))
        val header = b.readLine().split(",")
        val schema = Schema(header.map { Field.nullable(it, ArrowType.Utf8()) })
        return schema
    }

    override fun scan(projection: List<String>): Sequence<RecordBatch> {
        logger.fine("scan() projection=$projection")

        val b = BufferedReader(FileReader(filename))
        val header = b.readLine().split(",")
        val fileColumns = header.map { Field.nullable(it, ArrowType.Utf8()) }.toList()

        val projectionIndices = projection.map { name -> fileColumns.indexOfFirst { it.name == name } }

        val schema = when (projectionIndices.size) {
            0 -> Schema(fileColumns)
            else -> Schema(projectionIndices.map { fileColumns[it] })
        }

        return ReaderAsSequence(schema, projectionIndices, b, batchSize)
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

        val root = VectorSchemaRoot.create(schema, RootAllocator(Long.MAX_VALUE))
        root.allocateNew()
        root.rowCount = rows.size

        root.fieldVectors.withIndex().forEach { field ->
            when (field.value) {
                is VarCharVector -> rows.withIndex().forEach { row ->
                    val value = row.value[field.index]
                    (field.value as VarCharVector).set(row.index, value.toByteArray())
                }
                else -> TODO()
            }
            field.value.valueCount = rows.size
        }

        val batch = RecordBatch(schema, root.fieldVectors.map { ArrowFieldVector(it) })

        logger.fine("Created batch:\n${batch.toCSV()}")

        return batch
    }
}