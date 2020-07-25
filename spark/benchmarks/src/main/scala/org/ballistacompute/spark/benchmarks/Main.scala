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

package org.ballistacompute.spark.benchmarks

import org.ballistacompute.spark.benchmarks.nyctaxi.Benchmarks

/**
  * This benchmark is designed to be called as a Docker container.
  */
object Main {

  def main(args: Array[String]): Unit = {

    val format = sys.env("BENCH_FORMAT")
    val path = sys.env("BENCH_PATH")
    val sql = sys.env("BENCH_SQL")
    val resultFile = sys.env("BENCH_RESULT_FILE")
    val iterations = sys.env("BENCH_ITERATIONS").toInt

    Benchmarks.run(format, path, sql, iterations, resultFile)

  }

}
