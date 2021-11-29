package com.databricks.mosaic.expresions.format

import com.databricks.mosaic.expresions.format.mocks.expressions.{getHexRowsDf, wkt_rows}
import com.databricks.mosaic.functions._
import com.databricks.mosaic.test.SparkTest
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.{FunSuite, Matchers}


class HexToWKT extends FunSuite with SparkTest with Matchers {
  test("Conversion from Hex to WKT") {
    val spark = SparkSession.builder().getOrCreate()
    val df = getHexRowsDf(spark)
    register(spark)

    val wkt_results = df.withColumn("wkt", hex_to_wkt(col("hex"))).select("wkt").collect().map(_.toSeq)

    wkt_results should contain theSameElementsAs wkt_rows

    df.createTempView("format_testing")

    val wkt_results2 = spark.sql(
      "select hex_to_wkt(hex) as wkt from format_testing"
    ).collect().map(_.toSeq)

    wkt_results2 should contain theSameElementsAs wkt_rows
  }
}
