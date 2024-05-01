package com.databricks.labs.mosaic.test

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, TestSuite}
import org.apache.spark.sql._

trait SparkSuite extends TestSuite with BeforeAndAfterAll {

    var sparkConf: SparkConf =
        new SparkConf(false)
            .set("spark.executor.extraLibraryPath", "/usr/lib/gdal")
            .set("spark.sql.parquet.compression.codec", "uncompressed")
    @transient private var _sc: SparkContext = _
    @transient private var _spark: SparkSession = _

    def spark: SparkSession = _spark

    // noinspection ProcedureDefinition
    override def beforeAll() {
        startSpark()
        super.beforeAll()
    }

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

    private def startSpark(): Unit = {
        _sc = new SparkContext("local[4]", "test", sparkConf)
        _sc.setLogLevel("ERROR")
        _spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    }

    def sc: SparkContext = _sc

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

    def withConf(newConf: SparkConf): SparkSession = {
        stopSpark()
        sparkConf = newConf
        startSpark()
        _spark
    }

    protected object testImplicits extends SQLImplicits {
        protected override def _sqlContext: SQLContext = _spark.sqlContext
    }

}
