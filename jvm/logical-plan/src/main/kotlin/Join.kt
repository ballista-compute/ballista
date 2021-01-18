package org.ballistacompute.logical

import org.ballistacompute.datatypes.Field
import org.ballistacompute.datatypes.Schema

enum class JoinType{
    Inner, Left, Right
}
class Join(val left: LogicalPlan, val right: LogicalPlan, val join_type: JoinType,val on: List<Pair<String, String>>): LogicalPlan {
    override fun schema(): Schema {
        val duplicateKeys = on.filter{it -> it.first == it.second}.map{it.first}.toSet()
        val fields: List<Field> =  when (join_type){
            JoinType.Inner,JoinType.Left ->{
                val leftFields = left.schema().fields
                val rightFields = right.schema().fields.filter{ it -> !duplicateKeys.contains(it.name)}
                leftFields + rightFields
            }
             JoinType.Right->{
                 val leftFields = left.schema().fields.filter{ it -> !duplicateKeys.contains(it.name)}
                 val rightFields = right.schema().fields
                 leftFields + rightFields
            }
        }
        return Schema(fields)
    }

    override fun children(): List<LogicalPlan> {
        return listOf(this.left, this.right)
    }


}