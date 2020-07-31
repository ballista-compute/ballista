package org.ballistacompute.logical

import org.ballistacompute.datatypes.Action

data class QueryAction(val plan: LogicalPlan) : Action
