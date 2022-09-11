package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.Mosaic
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks.getBoroughs
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

trait MosaicFillBehaviors {
    this: AnyFlatSpec =>

    def wktMosaicFill(mosaicContext: => MosaicContext, spark: => SparkSession, resolution: Int): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs(mc)

        val mosaics = boroughs
            .select(
              mosaicfill(col("wkt"), resolution)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql(s"""
                    |select mosaicfill(wkt, $resolution) from boroughs
                    |""".stripMargin)
            .collect()

        boroughs.collect().length shouldEqual mosaics2.length
    }

    def wkbMosaicFill(mosaicContext: => MosaicContext, spark: => SparkSession, resolution: Int): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs(mc)

        val mosaics = boroughs
            .select(
              mosaicfill(convert_to(col("wkt"), "wkb"), resolution)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql(s"""
                    |select mosaicfill(convert_to_wkb(wkt), $resolution) from boroughs
                    |""".stripMargin)
            .collect()

        boroughs.collect().length shouldEqual mosaics2.length
    }

    def hexMosaicFill(mosaicContext: => MosaicContext, spark: => SparkSession, resolution: Int): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs(mc)

        val mosaics = boroughs
            .select(
              mosaicfill(convert_to(col("wkt"), "hex"), resolution)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql(s"""
                    |select mosaicfill(convert_to_hex(wkt), $resolution) from boroughs
                    |""".stripMargin)
            .collect()

        boroughs.collect().length shouldEqual mosaics2.length
    }

    def coordsMosaicFill(mosaicContext: => MosaicContext, spark: => SparkSession, resolution: Int): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs(mc)

        val mosaics = boroughs
            .select(
              mosaicfill(convert_to(col("wkt"), "coords"), resolution)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql(s"""
                    |select mosaicfill(convert_to_coords(wkt), $resolution) from boroughs
                    |""".stripMargin)
            .collect()

        boroughs.collect().length shouldEqual mosaics2.length
    }

    def wktMosaicFillKeepCoreGeom(mosaicContext: => MosaicContext, spark: => SparkSession, resolution: Int): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs(mc)

        val mosaics = boroughs
            .select(
              mosaicfill(col("wkt"), resolution, keepCoreGeometries = true)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql(s"""
                    |select mosaicfill(wkt, $resolution, true) from boroughs
                    |""".stripMargin)
            .collect()

        boroughs.collect().length shouldEqual mosaics2.length
    }

    def mosaicFillPoints(mosaicContext: => MosaicContext, spark: => SparkSession, resolution: Int): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val centroids: DataFrame = getBoroughs(mc)
            .withColumn("centroid", st_centroid2D(col("wkt")))
            .withColumn("centroid", st_point(col("centroid.x"), col("centroid.y")))

        val mosaics = centroids
            .select(
              mosaicfill(col("centroid"), resolution, keepCoreGeometries = true)
            )
            .collect()

        centroids.collect().length shouldEqual mosaics.length

        centroids.createOrReplaceTempView("centroids")

        val mosaics2 = spark
            .sql(s"""
                    |select mosaicfill(wkt, $resolution, true) from centroids
                    |""".stripMargin)
            .collect()

        centroids.collect().length shouldEqual mosaics2.length

        noException should be thrownBy
            spark.sql("select mosaicfill('MULTIPOINT((1 1) (2 2) (3 3))', 1, true)")
    }

    def auxiliaryMethods(mosaicContext: => MosaicContext, spark: => SparkSession, resolution: Int): Unit = {
        val mc = mosaicContext
        mosaicContext.register(spark)

        val geometryAPI = mc.getGeometryAPI
        val indexSystem = mc.getIndexSystem

        val pointGeom = geometryAPI.geometry("POINT(1 1)", "WKT")
        val multiPointGeom = geometryAPI.geometry("MULTIPOINT(2 2, 1 1)", "WKT")
        val lineStringGeom = geometryAPI.geometry("LINESTRING(1 1, 2 2)", "WKT")
        val multiLineStringGeom = geometryAPI.geometry("MULTILINESTRING((1 1, 2 2), (3 3, 4 4))", "WKT")

        noException should be thrownBy Mosaic.pointFill(pointGeom, resolution, indexSystem)
        noException should be thrownBy Mosaic.pointFill(multiPointGeom, resolution, indexSystem)
        noException should be thrownBy Mosaic.lineFill(lineStringGeom, resolution, indexSystem, geometryAPI)
        noException should be thrownBy Mosaic.lineFill(multiLineStringGeom, resolution, indexSystem, geometryAPI)

        an[Error] should be thrownBy Mosaic.pointFill(lineStringGeom, resolution, indexSystem)
        an[Error] should be thrownBy Mosaic.lineFill(pointGeom, resolution, indexSystem, geometryAPI)
    }

}
