package com.databricks.mosaic.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

trait SparkTest extends BeforeAndAfterAll { self: Suite =>
  @transient private var _sc: SparkContext = _
  @transient private var _spark: SparkSession = _

  def sc: SparkContext = _sc
  def spark: SparkSession = _spark

  var conf: SparkConf = new SparkConf(false)

  private def startSpark(): Unit = {
    _sc = new SparkContext("local[4]", "test", conf)
    _sc.setLogLevel("FATAL")
    _spark = SparkSession.builder.config(sc.getConf).getOrCreate()
  }

  private def stopSpark(): Unit = {
    if (_sc != null) {
      _sc.stop()
    }

    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")

    _sc = null
    _spark = null
  }

  override def beforeAll() {
    startSpark()
    super.beforeAll()
  }

  override def afterAll() {
    stopSpark()
    super.afterAll()
  }

  def restartSpark(): Unit = {
    stopSpark()
    startSpark()
  }

  def time[T](f: => T): (T, Double) = {
    val start = System.nanoTime()
    val ret = f
    val end = System.nanoTime()
    (ret, (end - start) / 1000000000.0)
  }

  def benchmark[T](f: => T)(n: Int = 200): Double = {
    val times = (0 until n).map(_ => {
      restartSpark()
      time(f)._2
    })
    1.0*times.sum/times.length
  }
}
