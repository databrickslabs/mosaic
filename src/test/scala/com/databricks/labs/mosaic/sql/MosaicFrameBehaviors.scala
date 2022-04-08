package com.databricks.labs.mosaic.sql

import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.POINT
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

trait MosaicFrameBehaviors { this: AnyFlatSpec =>

    def testConstructFromPoints(spark: => SparkSession): Unit = {
        val points = pointDf(spark)
        val mdf = MosaicFrame(points, "geometry")
        mdf.count() shouldBe points.count()
    }

    def testConstructFromPolygons(spark: => SparkSession): Unit = {
        val mdf = MosaicFrame(polyDf(spark).limit(10), "geometry")
        mdf.count() shouldBe 10
    }

    def testIndexPoints(spark: => SparkSession): Unit = {
        val points = pointDf(spark)
        val mdf = MosaicFrame(points, "geometry")
            .setIndexResolution(9)
            .applyIndex()
        mdf.columns.length shouldBe 20
        mdf.count shouldBe points.count
    }

    def testIndexPolygons(spark: => SparkSession): Unit = {
        val mdf = MosaicFrame(polyDf(spark).limit(10), "geometry")
            .setIndexResolution(9)
            .applyIndex(explodePolyFillIndexes = false)
        mdf.columns.length shouldBe polyDf(spark).columns.length + 1
        mdf.count() shouldBe 10
    }

    def testIndexPolygonsExplode(spark: => SparkSession): Unit = {
        val mdf = MosaicFrame(polyDf(spark).limit(10).withColumn("id", monotonically_increasing_id()), "geometry")
            .setIndexResolution(9)
            .applyIndex()
        mdf.columns.length shouldBe polyDf(spark).columns.length + 2
        mdf.groupBy("id").count().count() shouldBe 10
    }

    def testGetOptimalResolution(spark: => SparkSession): Unit = {
        val mdf = MosaicFrame(polyDf(spark), "geometry")
            .setIndexResolution(3)
            .applyIndex()

        val mdf2 = MosaicFrame(polyDf(spark), "geometry")
            .setIndexResolution(3)
            .applyIndex()

        mdf.getOptimalResolution(1d) shouldBe 9

        the[Exception] thrownBy mdf2.getOptimalResolution(0.1d) should have message
            MosaicSQLExceptions.NotEnoughGeometriesException.getMessage

        mdf.getOptimalResolution(10) shouldBe 9

        the[Exception] thrownBy mdf.getOptimalResolution should have message
            MosaicSQLExceptions.NotEnoughGeometriesException.getMessage
    }

    def testMultiplePointIndexResolutions(spark: => SparkSession): Unit = {
        val points = pointDf(spark)
        val mdf = MosaicFrame(points, "geometry")
        val resolutions = (6 to 10).toList
        val indexedMdf = resolutions
            .foldLeft(mdf)((d, i) => {
                d.setIndexResolution(i).applyIndex(dropExistingIndexes = false)
            })

        val indexList = indexedMdf.listIndexes
        indexList.length shouldBe resolutions.length
        noException should be thrownBy indexedMdf.dropAllIndexes
    }

    def testPointInPolyJoin(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val sc = spark
        val mc = mosaicContext
        import mc.functions.st_contains
        import sc.implicits._

        val points = pointDf(spark)
        val pointMdf = MosaicFrame(points, "geometry")
            .setIndexResolution(9)
            .applyIndex()
        val polyMdf = MosaicFrame(polyDf(spark), "geometry")
            .setIndexResolution(9)
            .applyIndex(explodePolyFillIndexes = false)

        val resultMdf = pointMdf.join(polyMdf)

        val expectedRowCount =
            points.alias("points").join(polyDf(spark).alias("polygon"), st_contains($"polygon.geometry", $"points.geometry")).count()

        resultMdf.count() shouldBe expectedRowCount
    }

    def testPointInPolyJoinExploded(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val sc = spark
        val mc = mosaicContext
        import mc.functions.st_contains
        import sc.implicits._

        val points = pointDf(spark)
        val pointMdf = MosaicFrame(points, "geometry")
            .setIndexResolution(9)
            .applyIndex()
        val polyMdf = MosaicFrame(polyDf(spark), "geometry")
            .setIndexResolution(9)
            .applyIndex(explodePolyFillIndexes = false)

        val resultMdf = pointMdf.join(polyMdf)

        val expectedRowCount =
            points.alias("points").join(polyDf(spark).alias("polygon"), st_contains($"polygon.geometry", $"points.geometry")).count()

        resultMdf.count() shouldBe expectedRowCount
    }

    def testPoorlyConfiguredPointInPolyJoins(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val sc = spark
        val mc = mosaicContext
        import mc.functions.st_contains
        import sc.implicits._

        val points = pointDf(spark).limit(100)
        val pointMdf_1 = MosaicFrame(points, "geometry")
            .setIndexResolution(8)
            .applyIndex()
        val pointMdf_2 = MosaicFrame(points, "geometry")
        val polyMdf = MosaicFrame(polyDf(spark), "geometry")
            .setIndexResolution(9)
            .applyIndex(explodePolyFillIndexes = false)

        val resultMdf_1 = pointMdf_1.join(polyMdf)
        val resultMdf_2 = pointMdf_2.join(polyMdf)

        val expectedRowCount =
            points.alias("points").join(polyDf(spark).alias("polygon"), st_contains($"polygon.geometry", $"points.geometry")).count()

        resultMdf_1.count() shouldBe expectedRowCount
        resultMdf_2.count() shouldBe expectedRowCount
    }

    def testPrettifier(spark: => SparkSession): Unit = {

        val points = pointDf(spark)
        val pointMdf = MosaicFrame(points, "geometry")
            .setIndexResolution(8)
            .applyIndex()

        noException should be thrownBy pointMdf.prettified()
    }

    def testExceptions(spark: => SparkSession): Unit = {
        val points = pointDf(spark)
        val pointMdf = MosaicFrame(points)
        val polyMdf = MosaicFrame(polyDf(spark), "geometry")

        the[Exception] thrownBy polyMdf.join(polyMdf) should have message
            MosaicSQLExceptions.MosaicFrameNotIndexed.getMessage
        the[Exception] thrownBy pointMdf.getFocalGeometryColumnName should have message
            MosaicSQLExceptions.NoGeometryColumnSet.getMessage
        the[Exception] thrownBy pointMdf.setIndexResolution(32) should have message
            MosaicSQLExceptions.BadIndexResolution(0, 16).getMessage
        the[Exception] thrownBy pointMdf
            .setGeometryColumn("geometry")
            .join(pointMdf.setGeometryColumn("geometry")) should have message
            MosaicSQLExceptions.SpatialJoinTypeNotSupported(POINT, POINT).getMessage

    }

}
