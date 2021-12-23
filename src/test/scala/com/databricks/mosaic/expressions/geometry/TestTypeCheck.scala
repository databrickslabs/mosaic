package com.databricks.mosaic.expressions.geometry

import org.scalatest.{FunSuite, Matchers}
import com.databricks.mosaic.test.SparkTest
import com.databricks.mosaic.mocks.{getHexRowsDf, getWKTRowsDf}
import com.databricks.mosaic.functions.{register, as_hex, st_geometrytype}
import org.apache.spark.sql.SparkSession

class TestTypeCheck extends FunSuite with Matchers with SparkTest {

  test("ST_GeometryType returns the correct geometry type string for WKT geometries") {
    val ss: SparkSession = spark
    import ss.implicits._
    register(spark)

    val df = getWKTRowsDf

    val results = df.select(st_geometrytype($"wkt").alias("result"))
      .orderBy("result")
      .as[String].collect().toList
    val expected = List("POLYGON", "MULTIPOLYGON", "MULTIPOLYGON", "POLYGON")

    results should contain theSameElementsAs expected

    df.createOrReplaceTempView("source")
    val sqlResults = spark.sql("select st_geometrytype(wkt) from source")
      .as[String].collect.toList

    sqlResults should contain theSameElementsAs expected
  }

  test("ST_GeometryType returns the correct geometry type string for hex-encoded WKB geometries") {
    val ss: SparkSession = spark
    import ss.implicits._
    register(spark)

    val df = getHexRowsDf.select(as_hex($"hex").alias("hex"))

    val results = df.select(st_geometrytype($"hex").alias("result"))
      .orderBy("result")
      .as[String].collect().toList

    val expected = List("POLYGON", "MULTIPOLYGON", "MULTIPOLYGON", "POLYGON")
    results should contain theSameElementsAs expected

    df.createOrReplaceTempView("source")
    val sqlResults = spark.sql("select st_geometrytype(hex) from source")
      .as[String].collect.toList

    sqlResults should contain theSameElementsAs expected
  }
}
