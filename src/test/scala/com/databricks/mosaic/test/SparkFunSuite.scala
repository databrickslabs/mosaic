package com.databricks.mosaic.test

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

import org.apache.spark.sql._

trait SparkFunSuite extends FunSuite with BeforeAndAfterAll {
    self: Suite =>

    var conf: SparkConf = new SparkConf(false)
    @transient private var _sc: SparkContext = _
    @transient private var _spark: SparkSession = _

    def spark: SparkSession = _spark

    // noinspection ProcedureDefinition
    override def beforeAll() {
        startSpark()
        super.beforeAll()
    }

    private def startSpark(): Unit = {
        _sc = new SparkContext("local[4]", "test", conf)
        _sc.setLogLevel("FATAL")
        _spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    }

    def sc: SparkContext = _sc

    // noinspection ProcedureDefinition
    override def afterAll() {
        stopSpark()
        super.afterAll()
    }

    def benchmark[T](f: => T)(n: Int = 200): Double = {
        val times = (0 until n).map(_ => {
            restartSpark()
            time(f)._2
        })
        1.0 * times.sum / times.length
    }

    def restartSpark(): Unit = {
        stopSpark()
        startSpark()
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

    def time[T](f: => T): (T, Double) = {
        val start = System.nanoTime()
        val ret = f
        val end = System.nanoTime()
        (ret, (end - start) / 1000000000.0)
    }

    protected object testImplicits extends SQLImplicits {
        protected override def _sqlContext: SQLContext = self.spark.sqlContext
    }

}
