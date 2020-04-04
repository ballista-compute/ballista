package org.ballistacompute.physical

import org.apache.arrow.vector.types.pojo.Schema
import org.ballistacompute.datasource.DataSource
import org.ballistacompute.datatypes.RecordBatch
import org.ballistacompute.datatypes.ColumnVector
import org.ballistacompute.datatypes.ArrowFieldVector
import org.ballistacompute.datatypes.LiteralValueVector
import org.ballistacompute.datatypes.ArrowVectorBuilder


/**
 * Scan a data source with optional push-down projection.
 */
class ScanExec(val ds: DataSource, val projection: List<String>) : PhysicalPlan {

    override fun schema(): Schema {
        val fields = ds.schema().fields
        return Schema(projection.map { name -> fields.findLast { it.name == name }})
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
