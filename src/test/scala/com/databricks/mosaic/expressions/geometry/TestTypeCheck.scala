package com.databricks.mosaic.expressions.geometry

import org.scalatest.{FunSuite, Matchers}
import com.databricks.mosaic.test.SparkTest
import com.databricks.mosaic.mocks.{getHexRowsDf, getWKTRowsDf}
import org.apache.spark.sql.functions.{col, rand}
import com.databricks.mosaic.functions.{as_hex, st_geometrytype}
import org.apache.spark.sql.SparkSession

class TestTypeCheck extends FunSuite with Matchers with SparkTest {

  test("ST_GeometryType returns the correct geometry type string for WKT geometries") {
    val ss: SparkSession = spark
    import ss.implicits._

    val df = getWKTRowsDf
      .withColumn("result", st_geometrytype(col("wkt")))
      .orderBy(rand(11)) //fix a specific ordering by fixing seed
      .select("result")

    val results = df.as[String].collect().toList
    val expected = List("POLYGON", "MULTIPOLYGON", "MULTIPOLYGON", "POLYGON")

    results should contain theSameElementsAs expected
  }

  test("ST_GeometryType returns the correct geometry type string for hex-encoded WKB geometries") {
    val ss: SparkSession = spark
    import ss.implicits._

    val df = getHexRowsDf
      .withColumn("result", st_geometrytype(as_hex(col("hex"))))
      .orderBy(rand(11)) //fix a specific ordering by fixing seed
      .select("result")

    val results = df.as[String].collect().toList

    val expected = List("POLYGON", "MULTIPOLYGON", "MULTIPOLYGON", "POLYGON")
    results should contain theSameElementsAs expected
  }
}
