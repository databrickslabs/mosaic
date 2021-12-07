package com.databricks.mosaic.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

trait SparkTest extends BeforeAndAfterAll { self: Suite =>
  @transient private var _sc: SparkContext = _
  @transient private var _spark: SparkSession = _

  def sc: SparkContext = _sc
  def spark: SparkSession = _spark

  var conf = new SparkConf(false)

  override def beforeAll() {
    _sc = new SparkContext("local[4]", "test", conf)
    _sc.setLogLevel("FATAL")
    _spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    super.beforeAll()
  }

  override def afterAll() {
    if (_sc != null) {
      _sc.stop()
    }

    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")

    _sc = null
    _spark = null
    super.afterAll()
  }
}
