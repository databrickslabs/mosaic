package com.databricks.labs.mosaic.sql

import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.POINT
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

trait MosaicFrameBehaviors { this: AnyFlatSpec =>

    def testConstructFromPoints(spark: => SparkSession, mosaicContext: => MosaicContext): Unit = {
        val points = pointDf(spark, mosaicContext)
        val mdf = MosaicFrame(points, "geometry")
        mdf.count() shouldBe points.count()
    }

    def testConstructFromPolygons(spark: => SparkSession, mosaicContext: => MosaicContext): Unit = {
        val mdf = MosaicFrame(polyDf(spark, mosaicContext).limit(10), "geometry")
        mdf.count() shouldBe 10
    }

    def testIndexPoints(spark: => SparkSession, mosaicContext: => MosaicContext, resolution: Int): Unit = {
        val points = pointDf(spark, mosaicContext)
        val mdf = MosaicFrame(points, "geometry")
            .setIndexResolution(resolution)
            .applyIndex()
        mdf.columns.length shouldBe 20
        mdf.count shouldBe points.count
    }

    def testIndexPolygons(spark: => SparkSession, mosaicContext: => MosaicContext, resolution: Int): Unit = {
        val mdf = MosaicFrame(polyDf(spark, mosaicContext).limit(10), "geometry")
            .setIndexResolution(resolution)
            .applyIndex(explodePolyFillIndexes = false)
        mdf.columns.length shouldBe polyDf(spark, mosaicContext).columns.length + 1
        mdf.count() shouldBe 10
    }

    def testIndexPolygonsExplode(spark: => SparkSession, mosaicContext: => MosaicContext, resolution: Int): Unit = {
        val mdf = MosaicFrame(polyDf(spark, mosaicContext).limit(10).withColumn("id", monotonically_increasing_id()), "geometry")
            .setIndexResolution(resolution)
            .applyIndex()
        mdf.columns.length shouldBe polyDf(spark, mosaicContext).columns.length + 2
        mdf.groupBy("id").count().count() shouldBe 10
    }

    def testGetOptimalResolution(
        spark: => SparkSession,
        mosaicContext: => MosaicContext,
        resolution: Int,
        expectedResolution: Int
    ): Unit = {
        val mdf = MosaicFrame(polyDf(spark, mosaicContext), "geometry")
            .setIndexResolution(resolution)
            .applyIndex()

        val mdf2 = MosaicFrame(polyDf(spark, mosaicContext).limit(1), "geometry")
            .setIndexResolution(resolution)
            .applyIndex()

        mdf.getOptimalResolution(1d) shouldBe expectedResolution

        the[Exception] thrownBy mdf2.getOptimalResolution(0.1d) should have message
            MosaicSQLExceptions.NotEnoughGeometriesException.getMessage

        mdf.getOptimalResolution(10) shouldBe expectedResolution

        the[Exception] thrownBy mdf.getOptimalResolution should have message
            MosaicSQLExceptions.NotEnoughGeometriesException.getMessage
    }

    def testMultiplePointIndexResolutions(
        spark: => SparkSession,
        mosaicContext: => MosaicContext,
        minResolution: Int,
        maxResolution: Int
    ): Unit = {
        val points = pointDf(spark, mosaicContext)
        val mdf = MosaicFrame(points, "geometry")
        val resolutions = (minResolution to maxResolution).toList
        val indexedMdf = resolutions
            .foldLeft(mdf)((d, i) => {
                d.setIndexResolution(i).applyIndex(dropExistingIndexes = false)
            })

        val indexList = indexedMdf.listIndexes
        indexList.length shouldBe resolutions.length
        noException should be thrownBy indexedMdf.dropAllIndexes
    }

    def testPointInPolyJoin(mosaicContext: => MosaicContext, spark: => SparkSession, resolution: Int): Unit = {
        val sc = spark
        val mc = mosaicContext
        import mc.functions.st_contains
        import sc.implicits._

        val points = pointDf(spark, mosaicContext)
        val pointMdf = MosaicFrame(points, "geometry")
            .setIndexResolution(resolution)
            .applyIndex()
        val polyMdf = MosaicFrame(polyDf(spark, mosaicContext), "geometry")
            .setIndexResolution(resolution)
            .applyIndex(explodePolyFillIndexes = false)

        val resultMdf = pointMdf.join(polyMdf)

        val expectedRowCount = points
            .alias("points")
            .join(polyDf(spark, mosaicContext).alias("polygon"), st_contains($"polygon.geometry", $"points.geometry"))
            .count()

        resultMdf.count() shouldBe expectedRowCount
    }

    def testPointInPolyJoinExploded(mosaicContext: => MosaicContext, spark: => SparkSession, resolution: Int): Unit = {
        val sc = spark
        val mc = mosaicContext
        import mc.functions.st_contains
        import sc.implicits._

        val points = pointDf(spark, mosaicContext)
        val pointMdf = MosaicFrame(points, "geometry")
            .setIndexResolution(resolution)
            .applyIndex()
        val polyMdf = MosaicFrame(polyDf(spark, mosaicContext), "geometry")
            .setIndexResolution(resolution)
            .applyIndex(explodePolyFillIndexes = false)

        val resultMdf = pointMdf.join(polyMdf)

        val expectedRowCount = points
            .alias("points")
            .join(polyDf(spark, mosaicContext).alias("polygon"), st_contains($"polygon.geometry", $"points.geometry"))
            .count()

        resultMdf.count() shouldBe expectedRowCount
    }

    def testPoorlyConfiguredPointInPolyJoins(mosaicContext: => MosaicContext, spark: => SparkSession, resolution: Int): Unit = {
        val sc = spark
        val mc = mosaicContext
        import mc.functions.st_contains
        import sc.implicits._

        val points = pointDf(spark, mosaicContext).limit(100)
        val pointMdf_1 = MosaicFrame(points, "geometry")
            .setIndexResolution(resolution - 1)
            .applyIndex()
        val pointMdf_2 = MosaicFrame(points, "geometry")
        val polyMdf = MosaicFrame(polyDf(spark, mosaicContext), "geometry")
            .setIndexResolution(resolution)
            .applyIndex(explodePolyFillIndexes = false)

        val resultMdf_1 = pointMdf_1.join(polyMdf)
        val resultMdf_2 = pointMdf_2.join(polyMdf)

        val expectedRowCount = points
            .alias("points")
            .join(polyDf(spark, mosaicContext).alias("polygon"), st_contains($"polygon.geometry", $"points.geometry"))
            .count()

        resultMdf_1.count() shouldBe expectedRowCount
        resultMdf_2.count() shouldBe expectedRowCount
    }

    def testPrettifier(spark: => SparkSession, mosaicContext: => MosaicContext, resolution: Int): Unit = {

        val points = pointDf(spark, mosaicContext)
        val pointMdf = MosaicFrame(points, "geometry")
            .setIndexResolution(resolution)
            .applyIndex()

        noException should be thrownBy pointMdf.prettified()
    }

    def testExceptions(spark: => SparkSession, mosaicContext: => MosaicContext, resolution: Int): Unit = {
        val points = pointDf(spark, mosaicContext)
        val pointMdf = MosaicFrame(points)
        val polyMdf = MosaicFrame(polyDf(spark, mosaicContext), "geometry")

        the[Exception] thrownBy polyMdf.join(polyMdf) should have message
            MosaicSQLExceptions.MosaicFrameNotIndexed.getMessage
        the[Exception] thrownBy pointMdf.getFocalGeometryColumnName should have message
            MosaicSQLExceptions.NoGeometryColumnSet.getMessage
        the[Exception] thrownBy pointMdf.setIndexResolution(resolution) should have message
            MosaicSQLExceptions
                .BadIndexResolution(mosaicContext.getIndexSystem.resolutions)
                .getMessage
        the[Exception] thrownBy pointMdf
            .setGeometryColumn("geometry")
            .join(pointMdf.setGeometryColumn("geometry")) should have message
            MosaicSQLExceptions.SpatialJoinTypeNotSupported(POINT, POINT).getMessage

    }

}
