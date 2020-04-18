package org.ballistacompute.physical

import org.ballistacompute.datasource.DataSource
import org.ballistacompute.datatypes.RecordBatch
import org.ballistacompute.datatypes.Schema


/**
 * Scan a data source with optional push-down projection.
 */
class ScanExec(val ds: DataSource, val projection: List<String>) : PhysicalPlan {

    override fun schema(): Schema {
        return ds.schema().select(projection)
    }

    override fun children(): List<PhysicalPlan> {
        return listOf()
    }

    override fun execute(): Sequence<RecordBatch> {
        return ds.scan(projection);
    }

    override fun toString(): String {
        return "ScanExec: schema=${schema()}, projection=$projection"
    }
}
