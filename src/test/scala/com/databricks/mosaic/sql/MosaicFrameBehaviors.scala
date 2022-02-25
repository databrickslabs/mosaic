package com.databricks.mosaic.sql

import com.databricks.mosaic.functions.MosaicContext
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import com.databricks.mosaic.mocks.getWKTRowsDf


trait MosaicFrameBehaviors { this: AnyFlatSpec =>
    def testConstructFromPoints(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import sc.implicits._
        import mc.functions._

        val df = getWKTRowsDf.where(st_geometrytype($"wkt") === "Point")
        val mdf = MosaicFrame(df, "wkt")
        mdf.df.count() shouldBe 1
    }

    def testConstructFromPolygons(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import sc.implicits._
        import mc.functions._

        val df = getWKTRowsDf.where(st_geometrytype($"wkt") === "Polygon")
        val mdf = MosaicFrame(df, "wkt")
        mdf.df.count() shouldBe 2
    }

    def testIndexPoints(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import sc.implicits._
        import mc.functions._

        val df = getWKTRowsDf.where(st_geometrytype($"wkt") === "Point")
        val mdf = MosaicFrame(df, "wkt")
        mdf.setIndexResolution(3)
        mdf.applyIndex()
        mdf.df.columns.length shouldBe 3
    }

    def testIndexPolygons(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import sc.implicits._
        import mc.functions._

        val df = getWKTRowsDf.where(st_geometrytype($"wkt") === "Polygon")
        val mdf = MosaicFrame(df, "wkt")
        mdf.setIndexResolution(3)
        mdf.applyIndex()
        mdf.df.columns.length shouldBe 5
    }

    def testPointInPolyJoin(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import sc.implicits._
        import mc.functions._

        val pointDf = getWKTRowsDf.where(st_geometrytype($"wkt") === "Point")
        val polyDf = getWKTRowsDf.where(st_geometrytype($"wkt") === "Polygon")
        val pointMdf = MosaicFrame(pointDf, "wkt")
        val polyMdf = MosaicFrame(polyDf, "wkt")
        pointMdf.setIndexResolution(3)
        pointMdf.applyIndex()
        polyMdf.setIndexResolution(3)
        polyMdf.applyIndex()
        val resultMdf = pointMdf.join(polyMdf)
        resultMdf.show()
    }
}
