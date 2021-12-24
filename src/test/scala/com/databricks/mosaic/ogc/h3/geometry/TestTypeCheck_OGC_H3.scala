package com.databricks.mosaic.ogc.h3.geometry

import com.databricks.mosaic.core.geometry.GeometryAPI.OGC
import com.databricks.mosaic.core.index.H3IndexSystem
import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.mocks.{getHexRowsDf, getWKTRowsDf}
import com.databricks.mosaic.test.SparkTest
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, rand}
import org.scalatest.{FunSuite, Matchers}

class TestTypeCheck_OGC_H3 extends FunSuite with Matchers with SparkTest {

  val mosaicContext: MosaicContext = MosaicContext(H3IndexSystem, OGC)
  import mosaicContext.functions._

  test("ST_GeometryType returns the correct geometry type string for WKT geometries") {
    val ss: SparkSession = spark
    import ss.implicits._

    val df = getWKTRowsDf
      .withColumn("result", st_geometrytype(col("wkt")))
      .orderBy(rand(11)) //fix a specific ordering by fixing seed
      .select("result")

    val results = df.as[String].collect().toList
    val expected = List("POLYGON", "MULTIPOLYGON", "MULTIPOLYGON", "POLYGON", "POINT")

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

    val expected = List("POLYGON", "MULTIPOLYGON", "MULTIPOLYGON", "POLYGON", "POINT")
    results should contain theSameElementsAs expected
  }
}
