package com.example

import org.apache.spark.sql.SparkSession

object PlaygroundApp {
  final val master = "local[*]"
  final val path = "data/example.csv"

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("Spark Playground Application")
      .master(master)
      .getOrCreate()

    val df = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load(path)

    val results = PlaygroundUtil.apply(df)
    results.show()
  }
}
