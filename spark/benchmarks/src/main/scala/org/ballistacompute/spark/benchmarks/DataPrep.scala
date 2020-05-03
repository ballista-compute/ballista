package org.ballistacompute.spark.benchmarks;

/**
  * Utility for converting CSV to Parquet and repartitioning files.
  */
object DataPrep {

  private def repartition(sourcePath: String, destPath: String, partitions: Int): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    spark.read.parquet(sourcePath)
      .repartition(partitions)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(destPath)
  }

  private def bulkConvert() = {

    val exec = Executors.newFixedThreadPool(24)

    for (year <- 2010 to 2018) {
      for (month <- 1 to 12) {
        val monthStr = "%02d".format(month)
        val csvPath = s"/home/andy/nyc-tripdata/source/yellow_tripdata_$year-$monthStr.csv"
        val parquetPath = s"/home/andy/nyc-tripdata/parquet/year=$year/month=$monthStr"
        if (File(parquetPath).exists) {
          println(s"$parquetPath exists")
        } else {
          println(s"Creating $parquetPath")
          exec.execute(new Runnable {
            override def run(): Unit = {
              convertToParquet(csvPath, parquetPath)
            }
          })
        }
      }
    }
  }

  /** Download the csv files from S3 ... this takes hours to download them all! */
  def downloadFiles(): Unit = {

    val exec = Executors.newFixedThreadPool(24)

    for (year <- 2010 to 2018) {
      for (month <- 1 to 12) {
        val filename = s"yellow_tripdata_$year-${"%02d".format(month)}.csv"
        if (File(filename).exists) {
          println(s"$filename exists", filename)
        } else {
          exec.execute(new Runnable {
            override def run(): Unit = {
              val url = s"https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_$year-${"%02d".format(month)}.csv"
              println(s"Downloading $url ...")
              //val cmd = Seq("wget", "--quiet", url).!
            }
          })
        }
      }
    }
  }

  /** Load CSV with schema inferred so that parquet file has correct types */
  def convertToParquet(csvPath: String, parquetPath: String) {

    val spark: SparkSession = SparkSession.builder
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvPath)

    df.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(parquetPath)


    df.printSchema()

  }

}
