package org.ballistacompute.datatypes


/*
  message ShuffleLocation {
    string job_uuid = 1;
    uint32 stage_id = 2;
    uint32 partition_id = 4;
    string executor_uuid = 5;
  }
 */

data class ShuffleLocation(val jobUuid: String, val stageId: Int, val partitionId: Int, val executionUuid: String)