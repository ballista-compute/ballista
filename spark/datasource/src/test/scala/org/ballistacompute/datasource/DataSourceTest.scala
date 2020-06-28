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

package io.andygrove.ballista.spark

import org.junit.{Ignore, Test}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.functions._

class DataSourceTest {

  @Test
  @Ignore
  def testSomething() {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .format("io.andygrove.ballista.spark.datasource")
      .option(
        "table",
        "/home/andy/git/andygrove/arrow/cpp/submodules/parquet-testing/data/alltypes_plain.parquet"
      )
      .option("host", "127.0.0.1")
      .option("port", "50051")
      .load()

    df.printSchema()

    val results = df.collect()
    results.foreach(println)

  }

}
