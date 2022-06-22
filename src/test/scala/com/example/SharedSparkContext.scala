package com.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SharedSparkContext extends BeforeAndAfterAll {
  self: Suite =>

  val SPARK_MASTER = "local[*]"

  val APP_NAME = "test"

  @transient private var _sc: SparkContext = _
  @transient private var _sqlContext: SparkSession = _

  def sc: SparkContext = _sc

  def spark: SparkSession = _sqlContext

  var conf = new SparkConf(false)

  override def beforeAll(): Unit = {
    _sc = new SparkContext(SPARK_MASTER, APP_NAME, conf)
    _sqlContext = SparkSession.builder.config(_sc.getConf).getOrCreate()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    _sc.stop()
    super.afterAll()
  }
}
