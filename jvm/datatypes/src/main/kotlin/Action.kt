package org.ballistacompute.datatypes

import org.ballistacompute.logical.LogicalPlan

interface Action

data class QueryAction(val plan: LogicalPlan) : Action

data class ShuffleIdAction(val shuffleId: ShuffleId) : Action