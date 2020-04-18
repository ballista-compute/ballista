package org.ballistacompute.datasource

import org.ballistacompute.datatypes.RecordBatch
import org.ballistacompute.datatypes.Schema


interface DataSource {

    /** Return the schema for the underlying data source */
    fun schema(): Schema

    /** Scan the data source, selecting the specified columns */
    fun scan(projection: List<String>): Sequence<RecordBatch>
}

