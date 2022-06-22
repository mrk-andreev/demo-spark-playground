package com.example

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

class PlaygroundUtilTest extends AnyFunSuite with SharedSparkContext {
  test("caseName") {
    val input = spark.createDataFrame(
      sc.parallelize(
        Seq(
          Row("a", "b"),
          Row("c", "d"),
        )
      ),
      new StructType()
        .add(StructField("old", StringType))
        .add(StructField("NAME", StringType))
    )

    val result = PlaygroundUtil.apply(input)
    val expected = spark.createDataFrame(
      sc.parallelize(
        Seq(
          Row("a", "b"),
          Row("c", "d"),
        )
      ),
      new StructType()
        .add(StructField("new", StringType))
        .add(StructField("NAME", StringType))
    )

    assert(result.schema === expected.schema)
    assert(result.collect() === expected.collect())
  }
}
