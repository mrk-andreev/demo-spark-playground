package com.example

import org.apache.spark.sql.DataFrame

object PlaygroundUtil {
  def apply(df: DataFrame): DataFrame = {
    df.withColumnRenamed("old", "new")
  }
}
