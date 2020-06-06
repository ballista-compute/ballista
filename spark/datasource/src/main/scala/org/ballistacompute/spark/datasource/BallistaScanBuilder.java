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

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;

public class BallistaScanBuilder implements ScanBuilder {

  TableMeta tableMeta;

  public BallistaScanBuilder(TableMeta tableMeta) {
    this.tableMeta = tableMeta;
  }

  @Override
  public Scan build() {
    return new BallistaScan(tableMeta);
  }
}
