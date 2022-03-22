package com.databricks.labs.mosaic.sql

import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

trait MosaicFrameBehaviors { this: AnyFlatSpec =>

    def testConstructFromPoints(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val limitedPoints = pointDf(spark).limit(100)
        val mdf = MosaicFrame(limitedPoints, "geometry")
        mdf.count() shouldBe limitedPoints.count()
    }

    def testConstructFromPolygons(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mdf = MosaicFrame(polyDf(spark), "geometry")
        mdf.count() shouldBe polyDf(spark).count()
    }

    def testIndexPoints(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val limitedPoints = pointDf(spark).limit(100)
        val mdf = MosaicFrame(limitedPoints, "geometry")
            .setIndexResolution(9)
            .applyIndex()
        mdf.columns.length shouldBe 20
        mdf.count shouldBe limitedPoints.count
    }

    def testIndexPolygons(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mdf = MosaicFrame(polyDf(spark), "geometry")
            .setIndexResolution(9)
            .applyIndex(explodePolyFillIndexes = false)
        mdf.columns.length shouldBe polyDf(spark).columns.length + 1
        mdf.count() shouldBe polyDf(spark).count()
    }

    def testIndexPolygonsExplode(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mdf = MosaicFrame(polyDf(spark).withColumn("id", monotonically_increasing_id()), "geometry")
            .setIndexResolution(9)
            .applyIndex()
        mdf.columns.length shouldBe polyDf(spark).columns.length + 2
        mdf.groupBy("id").count().count() shouldBe polyDf(spark).count()
    }

    def testGetOptimalResolution(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mdf = MosaicFrame(polyDf(spark), "geometry")
            .setIndexResolution(3)
            .applyIndex()
        val res = mdf.getOptimalResolution(1d)
        res shouldBe 9
    }

    def testMultiplePointIndexResolutions(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val limitedPoints = pointDf(spark).limit(100)
        val mdf = MosaicFrame(limitedPoints, "geometry")
        val resolutions = (6 to 10).toList
        val indexedMdf = resolutions
            .foldLeft(mdf)((d, i) => {
                d.setIndexResolution(i).applyIndex(dropExistingIndexes = false)
            })

        val indexList = indexedMdf.listIndexes
        indexList.length shouldBe resolutions.length
    }

    def testPointInPolyJoin(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val sc = spark
        val mc = mosaicContext
        import mc.functions.st_contains
        import sc.implicits._

        val limitedPoints = pointDf(spark).limit(100)
        val pointMdf = MosaicFrame(limitedPoints, "geometry")
            .setIndexResolution(9)
            .applyIndex()
        val polyMdf = MosaicFrame(polyDf(spark), "geometry")
            .setIndexResolution(9)
            .applyIndex(explodePolyFillIndexes = false)

        val resultMdf = pointMdf.join(polyMdf)

        val expectedRowCount =
            limitedPoints.alias("points").join(polyDf(spark).alias("polygon"), st_contains($"polygon.geometry", $"points.geometry")).count()

        resultMdf.count() shouldBe expectedRowCount
    }

    def testPointInPolyJoinExploded(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val sc = spark
        val mc = mosaicContext
        import mc.functions.st_contains
        import sc.implicits._

        val limitedPoints = pointDf(spark).limit(100)
        val pointMdf = MosaicFrame(limitedPoints, "geometry")
            .setIndexResolution(9)
            .applyIndex()
        val polyMdf = MosaicFrame(polyDf(spark), "geometry")
            .setIndexResolution(9)
            .applyIndex(explodePolyFillIndexes = false)

        val resultMdf = pointMdf.join(polyMdf)

        val expectedRowCount =
            limitedPoints.alias("points").join(polyDf(spark).alias("polygon"), st_contains($"polygon.geometry", $"points.geometry")).count()

        resultMdf.count() shouldBe expectedRowCount
    }

    def testPoorlyConfiguredPointInPolyJoins(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val sc = spark
        val mc = mosaicContext
        import mc.functions.st_contains
        import sc.implicits._

        val limitedPoints = pointDf(spark).limit(100)
        val pointMdf_1 = MosaicFrame(limitedPoints, "geometry")
            .setIndexResolution(8)
            .applyIndex()
        val pointMdf_2 = MosaicFrame(limitedPoints, "geometry")
        val polyMdf = MosaicFrame(polyDf(spark), "geometry")
            .setIndexResolution(9)
            .applyIndex(explodePolyFillIndexes = false)

        val resultMdf_1 = pointMdf_1.join(polyMdf)
        val resultMdf_2 = pointMdf_2.join(polyMdf)

        val expectedRowCount =
            limitedPoints.alias("points").join(polyDf(spark).alias("polygon"), st_contains($"polygon.geometry", $"points.geometry")).count()

        resultMdf_1.count() shouldBe expectedRowCount
        resultMdf_2.count() shouldBe expectedRowCount
    }

}
