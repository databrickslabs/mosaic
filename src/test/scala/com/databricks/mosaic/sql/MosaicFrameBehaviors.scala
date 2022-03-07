package com.databricks.mosaic.sql

import com.databricks.mosaic.functions.MosaicContext
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import com.databricks.mosaic.mocks.getWKTRowsDf
import org.apache.spark.sql.functions.to_json


trait MosaicFrameBehaviors { this: AnyFlatSpec =>
    def testConstructFromPoints(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import sc.implicits._
        import mc.functions._

        val df = getWKTRowsDf.where(st_geometrytype($"wkt") === "Point")
        val mdf = MosaicFrame(df, "wkt")
        mdf.count() shouldBe 1
    }

    def testConstructFromPolygons(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import sc.implicits._
        import mc.functions._

        val df = getWKTRowsDf.where(st_geometrytype($"wkt") === "Polygon")
        val mdf = MosaicFrame(df, "wkt")
        mdf.count() shouldBe 2
    }

    def testIndexPoints(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import sc.implicits._
        import mc.functions._

        val df = getWKTRowsDf.where(st_geometrytype($"wkt") === "Point")
        val mdf = MosaicFrame(df, "wkt")
            .setIndexResolution(3)
            .applyIndex()
        mdf.columns.length shouldBe 3
    }

    def testIndexPolygons(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import sc.implicits._
        import mc.functions._

        val df = getWKTRowsDf.where(st_geometrytype($"wkt") === "Polygon")
        val mdf = MosaicFrame(df, "wkt").setIndexResolution(3).applyIndex()
        mdf.columns.length shouldBe 5
    }

    def testGetOptimalResolution(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import sc.implicits._
        import mc.functions._

        val polyDf =
            spark.read
            .json("src/test/resources/NYC_Taxi_Zones.geojson")
            .withColumn("geometry", st_geomfromgeojson(to_json($"geometry")))
        val polyMdf = MosaicFrame(polyDf, "geometry")
        val res = polyMdf.getOptimalResolution(1d)
        res shouldBe 9
    }

    def testPointInPolyJoin(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import sc.implicits._
        import mc.functions._

        val pointDf =
            spark.read
                .options(Map(
                    "header" -> "true",
                    "inferSchema" -> "true"
                ))
                .csv("src/test/resources/nyctaxi_yellow_trips.csv")
                .withColumn("geometry", st_point($"pickup_longitude", $"pickup_latitude"))

        val polyDf =
            spark.read
                .json("src/test/resources/NYC_Taxi_Zones.geojson")
                .withColumn("geometry", st_geomfromgeojson(to_json($"geometry")))
        val pointMdf = MosaicFrame(pointDf, "geometry").setIndexResolution(9).applyIndex()
        val polyMdf = MosaicFrame(polyDf, "geometry").setIndexResolution(9).applyIndex()
        val resultMdf = pointMdf.join(polyMdf)
        resultMdf.show()
    }
}
