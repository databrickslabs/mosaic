package com.databricks.labs.mosaic.core.io

import com.databricks.labs.mosaic.test.mocks._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.SparkSession

trait GeoTiffBehaviours { this: AnyFlatSpec =>
    def testImageFromSparkBytes(spark: => SparkSession): Unit = {
        val ss = spark
        import ss.implicits._
        val imageBytesArray = modisDf(spark)
            .select($"content")
            .as[Array[Byte]]
            .collect
        val reader = new GeoTiffReader
        val images = imageBytesArray.map(reader.fromBytes)
        images.length shouldBe 22
    }
}
