package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks.getWKTRowsDf
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

trait ValidityBehaviors {
    this: AnyFlatSpec =>

    def validGeometries(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

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

    def invalidGeometries(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
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
          List("POLYGON((NaN 3, 3 4, 4 4, 4 3, 3 3))") // Invalid Coordinate
          // TODO: add ST_IsClosedRing to handle Esri/JTS behavior differences
          // List("POLYGON((0 0, 0 10, 10 10, 10 0))") // Ring Not Closed
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
        val results = df.select("result").as[Boolean].collect().toList

        all(results) should be(false)

        df.createOrReplaceTempView("source")
        val sqlResults = spark.sql("select st_isvalid(wkt) from source").collect.map(_.getBoolean(0)).toList

        all(sqlResults) should be(false)
    }

}
