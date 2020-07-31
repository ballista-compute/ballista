package org.ballistacompute.datatypes

interface Action

data class ShuffleIdAction(val shuffleId: ShuffleId) : Action