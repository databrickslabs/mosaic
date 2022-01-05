package com.databricks.mosaic.ogc.h3.expressions.geometry

import com.databricks.mosaic.core.geometry.api.GeometryAPI.OGC
import com.databricks.mosaic.core.index.H3IndexSystem
import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.mocks.getWKTRowsDf
import com.databricks.mosaic.test.SparkTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.{FunSuite, Matchers}

class TestGeometryValidity_OGC_H3 extends FunSuite with SparkTest with Matchers {

  val mosaicContext: MosaicContext = MosaicContext(H3IndexSystem, OGC)

  import mosaicContext.functions._

  test("Calling st_xmin() should return the minimum x value from all coordinates in the geometry") {
    val ss = spark
    import ss.implicits._

    val df = getWKTRowsDf.select(st_xmin(col("wkt")))
    val results = df.as[Double].collect()
    val expected = List(10.0, 0.0, 10.0, 10.0, -75.78033, 10.0, 10.0, 10.0)

    results should contain theSameElementsAs expected
  }

  test("Calling st_xmax() should return the maximum x value from all coordinates in the geometry") {
    val ss = spark
    import ss.implicits._

    val df = getWKTRowsDf.select(st_xmax(col("wkt")))
    val results = df.as[Double].collect()
    val expected = List(40.0, 2.0, 110.0, 45.0, -75.78033, 40.0, 40.0, 40.0)

    results should contain theSameElementsAs expected
  }

  test("Calling st_ymin() should return the minimum y value from all coordinates in the geometry") {
    val ss = spark
    import ss.implicits._

    val df = getWKTRowsDf.select(st_ymin(col("wkt")))
    val results = df.as[Double].collect()
    val expected = List(10.0, 0.0, 10.0, 5.0, 35.18937, 10.0, 10.0, 10.0)

    results should contain theSameElementsAs expected
  }

  test("Calling st_ymax() should return the maximum y value from all coordinates in the geometry") {
    val ss = spark
    import ss.implicits._

    val df = getWKTRowsDf.select(st_ymax(col("wkt")))
    val results = df.as[Double].collect()
    val expected = List(40.0, 2.0, 110.0, 60.0, 35.18937, 40.0, 40.0, 40.0)

    results should contain theSameElementsAs expected
  }

  test("Calling st_isvalid() on a valid geometry should return true.") {
    val ss = spark
    import ss.implicits._

    val df = getWKTRowsDf.select(st_isvalid(col("wkt")))
    val results = df.as[Boolean].collect().toSeq

    all(results) should be(true)
  }

  test("Calling st_isvalid() on an invalid geometry should return false.") {
    // create df with a selection of invalid geometries expressed as WKT

    val invalidGeometries = List(
      List(
        "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (15 15, 15 20, 20 20, 20 15, 15 15))"
      ), // Hole Outside Shell
      List(
        "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2), (3 3, 3 7, 7 7, 7 3, 3 3))"
      ), // Nested Holes,
      List(
        "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (5 0, 10 5, 5 10, 0 5, 5 0))"
      ), // Disconnected Interior
      List("POLYGON((0 0, 10 10, 0 10, 10 0, 0 0))"), // Self Intersection
      List(
        "POLYGON((5 0, 10 0, 10 10, 0 10, 0 0, 5 0, 3 3, 5 6, 7 3, 5 0))"
      ), // Ring Self Intersection
      List(
        "MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)),(( 2 2, 8 2, 8 8, 2 8, 2 2)))"
      ), // Nested Shells
      List(
        "MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)),((0 0, 10 0, 10 10, 0 10, 0 0)))"
      ), // Duplicated Rings
      List("POLYGON((2 2, 8 2))"), // Too Few Points
      List("POLYGON((NaN 3, 3 4, 4 4, 4 3, 3 3))"), // Invalid Coordinate
      // List("POLYGON((0 0, 0 10, 10 10, 10 0))") // Ring Not Closed
      // OGC API closes not closed geometries automatically
    )
    val rows = invalidGeometries.map { x => Row(x: _*) }
    val rdd = spark.sparkContext.makeRDD(rows)
    val schema = StructType(
      List(
        StructField("wkt", StringType)
      )
    )
    val invalidGeometriesDf = spark.createDataFrame(rdd, schema)

    val df = invalidGeometriesDf.withColumn("result", st_isvalid(col("wkt")))
    val results = df.collect().map(_.getBoolean(1)).toList

    all(results) should be(false)
  }

}
