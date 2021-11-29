package com.databricks.mosaic.expresions.format

import com.databricks.mosaic.expresions.format.mocks.expressions.{getWKTRowsDf, hex_rows}
import com.databricks.mosaic.expressions.format.Conversions
import com.databricks.mosaic.functions._
import com.databricks.mosaic.test.SparkTest
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.{FunSuite, Matchers}


class WKTToHex extends FunSuite with SparkTest with Matchers {
  test("Conversion from Hex to WKT") {
    val spark = SparkSession.builder().getOrCreate()
    val df = getWKTRowsDf(spark)
    register(spark)

    val results1 = df.withColumn("hex", wkt_to_hex(col("wkt"))).select("hex").collect().map(_.toSeq.head)
    val left_geoms1 = results1.map(_.asInstanceOf[String]).map(UTF8String.fromString).map(Conversions.hex2geom)
    val right_geoms = hex_rows.map(_.head).map(UTF8String.fromString).map(Conversions.hex2geom)

    left_geoms1 should contain theSameElementsAs right_geoms

    df.createTempView("format_testing")

    val results2 = spark.sql(
      "select wkt_to_hex(wkt) as hex from format_testing"
    ).collect().map(_.toSeq.head)
    val left_geoms2 = results2.map(_.asInstanceOf[String]).map(UTF8String.fromString).map(Conversions.hex2geom)

    left_geoms2 should contain theSameElementsAs right_geoms
  }
}
