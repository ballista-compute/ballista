package org.ballistacompute.spark.benchmarks

/**
 * This benchmark is designed to be called as a Docker container.
 */
object Main {

  def main(args: Array[String]): Unit = {

    val format = sys.env("BENCH_FORMAT")
    val path = sys.env("BENCH_PATH")
    val sql = sys.env("BENCH_SQL")
    val resultFile = sys.env("BENCH_RESULT_FILE")

    val iterations = 1
    val threads = "*"

    Benchmarks.run(format,
      path,
      sql,
      iterations,
      threads,
      resultFile)

  }

}

