package org.ballistacompute.datatypes

/*
  message ShuffleId {
    string job_uuid = 1;
    uint32 stage_id = 2;
    uint32 partition_id = 4;
  }
 */

data class ShuffleId(val jobUuid: String, val stageId: Int, val partitionId: Int)