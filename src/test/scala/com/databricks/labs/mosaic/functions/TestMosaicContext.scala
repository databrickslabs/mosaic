package com.databricks.labs.mosaic.functions

import scala.sys.process._
import scala.util.Try

import com.databricks.labs.mosaic.{ESRI, GDAL, H3}
import com.databricks.labs.mosaic.test.SparkSuite
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

class TestMosaicContext extends AnyFlatSpec with SparkSuite {

    "MosaicContext" should "install GDAL on the driver and executors" in {
        val sc = spark.sparkContext
        val mc = MosaicContext.build(indexSystem = H3, geometryAPI = ESRI, rasterAPI = GDAL)
        mc.enableGDAL(spark)

        val checkCmd = "gdalinfo --version"
        val resultDriver = Try(checkCmd.!!).getOrElse("")
        resultDriver should not be ""
        resultDriver should include("GDAL")

        val numExecutors = sc.getExecutorMemoryStatus.size - 1
        val resultExecutors = Try(sc.parallelize(1 to numExecutors).pipe(checkCmd).collect).getOrElse(Array[String]())
        resultExecutors.length should not be 0
        resultExecutors.foreach(s => s should include("GDAL"))

    }

}
