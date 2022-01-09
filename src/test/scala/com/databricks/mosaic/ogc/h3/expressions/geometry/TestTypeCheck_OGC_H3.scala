package com.databricks.mosaic.ogc.h3.expressions.geometry

import com.databricks.mosaic.core.geometry.api.GeometryAPI.OGC
import com.databricks.mosaic.core.index.H3IndexSystem
import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.mocks.{getHexRowsDf, getWKTRowsDf}
import com.databricks.mosaic.test.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.{FunSuite, Matchers}

class TestTypeCheck_OGC_H3 extends SparkFunSuite with Matchers {

  val mosaicContext: MosaicContext = MosaicContext(H3IndexSystem, OGC)

  import mosaicContext.functions._

  test("ST_GeometryType returns the correct geometry type string for WKT geometries") {
    val ss: SparkSession = spark
    import ss.implicits._

    val df = getWKTRowsDf
      .withColumn("result", st_geometrytype(col("wkt")))
      .select("result")

    val results = df.as[String].collect().toList.sorted
    val expected = List("LINESTRING", "MULTILINESTRING", "MULTIPOINT", "MULTIPOLYGON", "MULTIPOLYGON", "POINT", "POLYGON", "POLYGON")

    results should contain theSameElementsInOrderAs expected
  }

  test("ST_GeometryType returns the correct geometry type string for hex-encoded WKB geometries") {
    val ss: SparkSession = spark
    import ss.implicits._

    val df = getHexRowsDf
      .withColumn("result", st_geometrytype(as_hex(col("hex"))))
      .select("result")

    val results = df.as[String].collect().toList.sorted

    val expected = List("LINESTRING", "MULTILINESTRING", "MULTIPOINT", "MULTIPOLYGON", "MULTIPOLYGON", "POINT", "POLYGON", "POLYGON")
    results should contain theSameElementsInOrderAs expected
  }
}
