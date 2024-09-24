package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.{MosaicSpatialQueryTest, mocks}
import com.databricks.labs.mosaic.test.mocks.getBoroughs
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{size => arrayColumnSize}
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers._

import scala.util.{Failure, Success, Try}

//noinspection ScalaDeprecation
trait MosaicFillBehaviors extends MosaicSpatialQueryTest {

    def wktMosaicFill(mosaicContext: MosaicContext): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val resolution = mc.getIndexSystem match {
            case H3IndexSystem  => 11
            case BNGIndexSystem => 4
            case _ => 4
        }

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

        noException should be thrownBy mosaicfill(col("wkt"), resolution, lit(true))
    }

    def wkbMosaicFill(mosaicContext: MosaicContext): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val resolution = mc.getIndexSystem match {
            case H3IndexSystem  => 11
            case BNGIndexSystem => 4
            case _ => 4
        }

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

    def hexMosaicFill(mosaicContext: MosaicContext): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val resolution = mc.getIndexSystem match {
            case H3IndexSystem  => 11
            case BNGIndexSystem => 4
            case _ => 4
        }

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

    def coordsMosaicFill(mosaicContext: MosaicContext): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val resolution = mc.getIndexSystem match {
            case H3IndexSystem  => 11
            case BNGIndexSystem => 4
            case _ => 4
        }

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

    def wktMosaicFillKeepCoreGeom(mosaicContext: MosaicContext): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val resolution = mc.getIndexSystem match {
            case H3IndexSystem  => 11
            case BNGIndexSystem => 4
            case _ => 10
        }

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

    def wktMosaicTessellate(mosaicContext: MosaicContext): Unit = {
        val mc = mosaicContext
        val sc = spark
        mc.register(sc)
        import sc.implicits._
        import mc.functions._

        val geom = Seq("POLYGON ((-3.26 52.72, -3.20 52.71, -3.21 52.75, -3.26 52.75, -3.26 52.72))").toDF("wkt")
        val geomProjected = Try(mc.getIndexSystem.crsID) match {
            case Success(crsID) => geom.select(st_updatesrid(col("wkt"), lit(4326), lit(crsID)).alias("wkt"))
            case Failure(_) => geom
        }
        val mosaics = geomProjected
            .select(
              grid_tessellate( col("wkt"), 3).alias("tessellation")
            )
            .select(arrayColumnSize($"tessellation.chips").alias("number_of_chips"))
            .select($"number_of_chips")
            .collect()
            .map(_.getInt(0))

        mosaics.foreach { case i => assert(i > 0, "tessellation has no chips") }
    }

    def mosaicFillPoints(mosaicContext: MosaicContext): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val resolution = mc.getIndexSystem match {
            case H3IndexSystem  => 11
            case BNGIndexSystem => 4
            case _ => 10
        }

        val boroughs: DataFrame = getBoroughs(mc)
            .withColumn("centroid", st_centroid(col("wkt")))

        val mosaics = boroughs
            .select(
              mosaicfill(col("centroid"), resolution, keepCoreGeometries = true)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length
    }

    def mosaicFillMultiPoints(mosaicContext: MosaicContext): Unit = {
        val sc = spark
        val mc = mosaicContext
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val resolution = mc.getIndexSystem match {
            case H3IndexSystem  => 11
            case BNGIndexSystem => 4
            case _ => 10
        }

        val geometryAPI = mc.getGeometryAPI

        val boroughs: DataFrame = getBoroughs(mc)
            .select("wkt")
            .as[String]
            .map(wkt => {
                val geom = geometryAPI.geometry(wkt, "WKT")
                val boundaryPoints = geom.getBoundary.getShellPoints.flatten
                val multiPoint = boundaryPoints.tail.fold(boundaryPoints.head.asInstanceOf[MosaicGeometry])((mp, p) => mp.union(p))
                multiPoint.toWKT
            })
            .toDF("wkt")

        val mosaics = boroughs
            .select(
              mosaicfill(col("wkt"), resolution, keepCoreGeometries = true)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length
    }

    def columnFunctionSignatures(mosaicContext: MosaicContext): Unit = {
        val funcs = mosaicContext.functions
        noException should be thrownBy funcs.grid_tessellate(col("wkt"), lit(3))
        noException should be thrownBy funcs.grid_tessellate(col("wkt"), 3)
        noException should be thrownBy funcs.grid_tessellate(col("wkt"), 3, keepCoreGeometries = true)
        noException should be thrownBy funcs.grid_tessellate(col("wkt"), lit(3), keepCoreGeometries = true)
        noException should be thrownBy funcs.grid_tessellate(col("wkt"), lit(3), lit(false))
        // legacy API
        noException should be thrownBy funcs.mosaicfill(col("wkt"), lit(3))
        noException should be thrownBy funcs.mosaicfill(col("wkt"), 3)
        noException should be thrownBy funcs.mosaicfill(col("wkt"), 3, keepCoreGeometries = true)
        noException should be thrownBy funcs.mosaicfill(col("wkt"), lit(3), keepCoreGeometries = true)
        noException should be thrownBy funcs.mosaicfill(col("wkt"), lit(3), lit(false))
    }

    def auxiliaryMethods(mosaicContext: MosaicContext): Unit = {
        val mc = mosaicContext
        mosaicContext.register(spark)
        val sc = spark
        import sc.implicits._

        val wkt = mocks.getWKTRowsDf(mc.getIndexSystem).limit(1).select("wkt").as[String].collect().head
        val resExpr = mc.getIndexSystem match {
            case H3IndexSystem  => lit(mc.getIndexSystem.resolutions.head).expr
            case BNGIndexSystem => lit("100m").expr
            case _ => lit(4).expr
        }

        val mosaicfillExpr = MosaicFill(
          lit(wkt).expr,
          resExpr,
          lit(false).expr,
          mc.getIndexSystem,
          mc.getGeometryAPI.name
        )

        mosaicfillExpr.first shouldEqual lit(wkt).expr
        mosaicfillExpr.second shouldEqual resExpr
        mosaicfillExpr.third shouldEqual lit(false).expr

        mc.getIndexSystem match {
            case H3IndexSystem  => mosaicfillExpr.inputTypes should contain theSameElementsAs
                    Seq(StringType, IntegerType, BooleanType)
            case BNGIndexSystem => mosaicfillExpr.inputTypes should contain theSameElementsAs
                    Seq(StringType, StringType, BooleanType)
            case _ => mosaicfillExpr.inputTypes should contain theSameElementsAs
              Seq(StringType, IntegerType, BooleanType)
        }

        val badExpr = MosaicFill(
          lit(10).expr,
          resExpr,
          lit(false).expr,
          mc.getIndexSystem,
          mc.getGeometryAPI.name
        )

        an[Error] should be thrownBy badExpr.inputTypes

        // legacy API def tests
        noException should be thrownBy mc.functions.mosaicfill(lit(""), lit(5))
        noException should be thrownBy mc.functions.mosaicfill(lit(""), 5)
        noException should be thrownBy mc.functions.mosaicfill(lit(""), lit(5), lit(true))
        noException should be thrownBy mc.functions.mosaicfill(lit(""), lit(5), keepCoreGeometries = true)
        noException should be thrownBy mc.functions.mosaicfill(lit(""), 5, keepCoreGeometries = true)

        noException should be thrownBy mosaicfillExpr.makeCopy(mosaicfillExpr.children.toArray)
    }

}
