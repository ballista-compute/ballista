package org.ballistacompute.datasource

import org.apache.arrow.vector.types.pojo.Schema
import org.ballistacompute.datatypes.RecordBatch

class InMemoryDataSource(val schema: Schema, val data: List<RecordBatch>): DataSource {

    override fun schema(): Schema {
        return schema
    }

    override fun scan(columns: List<String>): Sequence<RecordBatch> {
        return data.asSequence()
    }
}