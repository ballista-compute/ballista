package org.ballistacompute.logical

import org.ballistacompute.datasource.DataSource
import org.ballistacompute.datatypes.Schema

/** Represents a scan of a data source */
class Scan(val path: String, val dataSource: DataSource, val projection: List<String>): LogicalPlan {

    val schema = deriveSchema()

    override fun schema(): Schema {
        return schema
    }

    private fun deriveSchema() : Schema {
        val schema = dataSource.schema()
        if (projection.isEmpty()) {
            return schema
        } else {
            return schema.select(projection)
        }
    }

    override fun children(): List<LogicalPlan> {
        return listOf()
    }

    override fun toString(): String {
        return if (projection.isEmpty()) {
            "Scan: $path; projection=None"
        } else {
            "Scan: $path; projection=$projection"
        }
    }

}