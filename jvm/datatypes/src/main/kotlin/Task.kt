package org.ballistacompute.datatypes

import org.ballistacompute.physical.PhysicalPlan

/*
  message Task {
    string job_uuid = 1;
    uint32 stage_id = 2;
    uint32 task_id = 3;
    uint32 partition_id = 4;
    PhysicalPlanNode plan = 5;
    // The task could need to read shuffle output from another task
    repeated ShuffleLocation shuffle_loc = 6;
  }
 */
data class Task (val jobUuid: String, val stageId: Int, val taskId: Int, val partitionId: Int, val plan: PhysicalPlan)