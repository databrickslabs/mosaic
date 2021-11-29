package com.databricks.mosaic.test

import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest._

trait SparkTest extends BeforeAndAfterAll { self: Suite =>
  @transient private var _sc: SparkContext = _

  def sc: SparkContext = _sc

  var conf = new SparkConf(false)

  override def beforeAll() {
    _sc = new SparkContext("local[4]", "test", conf)
    _sc.setLogLevel("FATAL")
    super.beforeAll()
  }

  override def afterAll() {
    if (_sc != null) {
      _sc.stop()
    }

    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")

    _sc = null
    super.afterAll()
  }
}
