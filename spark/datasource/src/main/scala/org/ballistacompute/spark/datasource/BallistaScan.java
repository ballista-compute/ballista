// Copyright 2020 Andy Grove
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.ballistacompute.spark.datasource;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.streaming.ContinuousStream;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.StructType;

public class BallistaScan implements Scan {

  TableMeta tableMeta;

  public BallistaScan(TableMeta tableMeta) {
    this.tableMeta = tableMeta;
  }

  @Override
  public StructType readSchema() {
    return tableMeta.schema;
  }

  @Override
  public String description() {
    return tableMeta.tableName;
  }

  @Override
  public Batch toBatch() {
    return new BallistaBatch(tableMeta);
  }

  @Override
  public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ContinuousStream toContinuousStream(String checkpointLocation) {
    throw new UnsupportedOperationException();
  }
}
