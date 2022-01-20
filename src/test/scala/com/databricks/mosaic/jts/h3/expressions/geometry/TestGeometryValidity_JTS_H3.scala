package com.databricks.mosaic.jts.h3.expressions.geometry

import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

import com.databricks.mosaic.core.geometry.api.GeometryAPI.JTS
import com.databricks.mosaic.core.index.H3IndexSystem
import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.mocks.getWKTRowsDf
import com.databricks.mosaic.test.SparkFlatSpec

class TestGeometryValidity_JTS_H3 extends SparkFlatSpec with Matchers {

    val mosaicContext: MosaicContext = MosaicContext.build(H3IndexSystem, JTS)

    import mosaicContext.functions._

    it should "Calling st_xmin() should return the minimum x value from all coordinates in the geometry" in {
        mosaicContext.register(spark)
        val ss = spark
        import ss.implicits._

        val df = getWKTRowsDf.orderBy("id")
        val results = df
            .select(st_xmin(col("wkt")))
            .as[Double]
            .collect()

        val expected = List(10.0, 0.0, 10.0, 10.0, -75.78033, 10.0, 10.0, 10.0)

        results should contain theSameElementsAs expected

        df.createOrReplaceTempView("source")
        val sqlResults = spark
            .sql("select st_xmin(wkt) from source")
            .as[Double]
            .collect()

        sqlResults should contain theSameElementsAs expected

    }

    it should "Calling st_xmax() should return the maximum x value from all coordinates in the geometry" in {
        mosaicContext.register(spark)
        val ss = spark
        import ss.implicits._

        val df = getWKTRowsDf.orderBy("id")
        val results = df.select(st_xmax(col("wkt"))).as[Double].collect()
        val expected = List(40.0, 2.0, 110.0, 45.0, -75.78033, 40.0, 40.0, 40.0)

        results should contain theSameElementsAs expected

        df.createOrReplaceTempView("source")
        val sqlResults = spark
            .sql("select st_xmax(wkt) from source")
            .as[Double]
            .collect()

        sqlResults should contain theSameElementsAs expected
    }

    it should "Calling st_ymin() should return the minimum y value from all coordinates in the geometry" in {
        mosaicContext.register(spark)
        val ss = spark
        import ss.implicits._

        val df = getWKTRowsDf.orderBy("id")
        val results = df.select(st_ymin(col("wkt"))).as[Double].collect()
        val expected = List(10.0, 0.0, 10.0, 5.0, 35.18937, 10.0, 10.0, 10.0)

        results should contain theSameElementsAs expected

        df.createOrReplaceTempView("source")
        val sqlResults = spark
            .sql("select st_ymin(wkt) from source")
            .as[Double]
            .collect()

        sqlResults should contain theSameElementsAs expected
    }

    it should "Calling st_ymax() should return the maximum y value from all coordinates in the geometry" in {
        mosaicContext.register(spark)
        val ss = spark
        import ss.implicits._

        val df = getWKTRowsDf.orderBy("id")
        val results = df.select(st_ymax(col("wkt"))).as[Double].collect()
        val expected = List(40.0, 2.0, 110.0, 60.0, 35.18937, 40.0, 40.0, 40.0)

        results should contain theSameElementsAs expected

        df.createOrReplaceTempView("source")
        val sqlResults = spark
            .sql("select st_ymax(wkt) from source")
            .as[Double]
            .collect()

        sqlResults should contain theSameElementsAs expected
    }

    it should "Calling st_isvalid() on a valid geometry should return true." in {
        mosaicContext.register(spark)
        val ss = spark
        import ss.implicits._

        val df = getWKTRowsDf.orderBy("id")
        val results = df.select(st_isvalid(col("wkt"))).as[Boolean].collect().toSeq

        all(results) should be(true)

        df.createOrReplaceTempView("source")
        val sqlResults = spark
            .sql("select st_isvalid(wkt) from source")
            .as[Boolean]
            .collect
            .toSeq

        all(sqlResults) should be(true)
    }

    it should "Calling st_isvalid() on an invalid geometry should return false." in {
        // create df with a selection of invalid geometries expressed as WKT
        mosaicContext.register(spark)

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
          List("POLYGON((0 0, 0 10, 10 10, 10 0))") // Ring Not Closed
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

        df.createOrReplaceTempView("source")
        val sqlResults = spark.sql("select st_isvalid(wkt) from source").collect.map(_.getBoolean(0)).toList

        all(sqlResults) should be(false)
    }

}
