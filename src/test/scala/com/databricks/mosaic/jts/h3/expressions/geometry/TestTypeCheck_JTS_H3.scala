package com.databricks.mosaic.jts.h3.expressions.geometry

import com.databricks.mosaic.core.geometry.api.GeometryAPI.JTS
import com.databricks.mosaic.core.index.H3IndexSystem
import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.mocks.{getHexRowsDf, getWKTRowsDf}
import com.databricks.mosaic.test.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSuite, Matchers}

class TestTypeCheck_JTS_H3 extends SparkFunSuite with Matchers {

  val mosaicContext: MosaicContext = MosaicContext(H3IndexSystem, JTS)

  import mosaicContext.functions._

  test("ST_GeometryType returns the correct geometry type string for WKT geometries") {
    mosaicContext.register(spark)
    val ss: SparkSession = spark
    import ss.implicits._

    val df = getWKTRowsDf

    val results = df.select(st_geometrytype($"wkt").alias("result"))
      .as[String].collect().toList.sorted
    val expected = List("LINESTRING", "MULTILINESTRING", "MULTIPOINT", "MULTIPOLYGON", "MULTIPOLYGON", "POINT", "POLYGON", "POLYGON")

    results should contain theSameElementsInOrderAs expected

    df.createOrReplaceTempView("source")
    val sqlResults = spark.sql("select st_geometrytype(wkt) from source")
      .as[String].collect.toList.sorted

    sqlResults should contain theSameElementsInOrderAs expected
  }

  test("ST_GeometryType returns the correct geometry type string for hex-encoded WKB geometries") {
    mosaicContext.register(spark)
    val ss: SparkSession = spark
    import ss.implicits._

    val df = getHexRowsDf.select(as_hex($"hex").alias("hex"))

    val results = df.select(st_geometrytype($"hex").alias("result"))
      .orderBy("result")
      .as[String].collect().toList.sorted

    val expected = List("LINESTRING", "MULTILINESTRING", "MULTIPOINT", "MULTIPOLYGON", "MULTIPOLYGON", "POINT", "POLYGON", "POLYGON")
    results should contain theSameElementsInOrderAs expected

    df.createOrReplaceTempView("source")
    val sqlResults = spark.sql("select st_geometrytype(hex) from source")
      .as[String].collect.toList.sorted

    sqlResults should contain theSameElementsInOrderAs expected
  }
}
