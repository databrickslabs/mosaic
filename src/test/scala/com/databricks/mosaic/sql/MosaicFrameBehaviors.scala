package com.databricks.mosaic.sql

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.SparkSession

import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.sql.mocks._

trait MosaicFrameBehaviors { this: AnyFlatSpec =>

    def testConstructFromPoints(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val limitedPoints = pointDf.limit(100)
        val mdf = MosaicFrame(limitedPoints, "geometry")
        mdf.count() shouldBe limitedPoints.count()
    }

    def testConstructFromPolygons(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mdf = MosaicFrame(polyDf, "geometry")
        mdf.count() shouldBe polyDf.count()
    }

    def testIndexPoints(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val limitedPoints = pointDf.limit(100)
        val mdf = MosaicFrame(limitedPoints, "geometry")
            .setIndexResolution(9)
            .applyIndex()
        mdf.columns.length shouldBe 20
        mdf.count shouldBe limitedPoints.count
    }

    def testIndexPolygons(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mdf = MosaicFrame(polyDf, "geometry")
            .setIndexResolution(9)
            .applyIndex(explodePolyFillIndexes = false)
        mdf.columns.length shouldBe polyDf.columns.length + 1
        mdf.count() shouldBe polyDf.count()
    }

    def testIndexPolygonsExplode(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mdf = MosaicFrame(polyDf, "geometry")
            .setIndexResolution(9)
            .applyIndex()
        mdf.columns.length shouldBe polyDf.columns.length + 3
        mdf.count() shouldBe 11986
    }

    def testGetOptimalResolution(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mdf = MosaicFrame(polyDf, "geometry")
            .setIndexResolution(3)
            .applyIndex()
        val res = mdf.getOptimalResolution(1d)
        res shouldBe 9
    }

    def testMultiplePointIndexResolutions(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val limitedPoints = pointDf.limit(100)
        val mdf = MosaicFrame(limitedPoints, "geometry")
        val resolutions = (6 to 10).toList
        val indexedMdf = resolutions
            .foldLeft(mdf)((d, i) => {
                d.setIndexResolution(i).applyIndex(dropExistingIndexes = false)
            })

        val indexDf = indexedMdf.listIndexes
        indexDf.count() shouldBe resolutions.length
    }

    def testPointInPolyJoin(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val sc = spark
        val mc = mosaicContext
        import sc.implicits._
        import mc.functions.st_contains

        val limitedPoints = pointDf.limit(100)
        val pointMdf = MosaicFrame(limitedPoints, "geometry")
            .setIndexResolution(9)
            .applyIndex()
        val polyMdf = MosaicFrame(polyDf, "geometry")
            .setIndexResolution(9)
            .applyIndex()

        val resultMdf = pointMdf.join(polyMdf)

        val expectedRowCount =
            limitedPoints.alias("points").join(polyDf.alias("polygon"), st_contains($"polygon.geometry", $"points.geometry")).count()

        resultMdf.count() shouldBe expectedRowCount
    }

}
