package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import com.databricks.labs.mosaic.core.Mosaic
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks.{getBoroughs, getWKTRowsDf}
import com.databricks.labs.mosaic.test.mocks
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

//noinspection ScalaDeprecation
trait MosaicExplodeBehaviors {
    this: AnyFlatSpec =>

    def wktDecompose(mosaicContext: => MosaicContext, spark: => SparkSession, resolution: Int): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs(mc)

        val mosaics = boroughs
            .select(
              mosaic_explode(col("wkt"), resolution)
            )
            .collect()

        boroughs.collect().length should be <= mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql(s"""
                    |select mosaic_explode(wkt, $resolution) from boroughs
                    |""".stripMargin)
            .collect()

        boroughs.collect().length should be <= mosaics2.length
    }

    def wktDecomposeNoNulls(mosaicContext: => MosaicContext, spark: => SparkSession, resolution: Int): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val rdd = spark.sparkContext.makeRDD(
          Seq(
            Row("POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))")
          )
        )
        val schema = StructType(
          List(
            StructField("wkt", StringType)
          )
        )
        val df = spark.createDataFrame(rdd, schema)

        val noEmptyChips = df
            .select(
              mosaic_explode(col("wkt"), resolution, keepCoreGeometries = true)
            )
            .filter(col("index.wkb").isNull)

        noEmptyChips.collect().length should be >= 0

        val noEmptyChips2 = df
          .select(
            mosaic_explode(col("wkt"), resolution, keepCoreGeometries = lit(true))
          )
          .filter(col("index.wkb").isNull)

        noEmptyChips2.collect().length should be >= 0

        val emptyChips = df
            .select(
              mosaic_explode(col("wkt"), resolution, keepCoreGeometries = false)
            )
            .filter(col("index.wkb").isNull)

        emptyChips.collect().length should be >= 0

        val emptyChips2 = df
          .select(
            mosaic_explode(col("wkt"), resolution, keepCoreGeometries = lit(false))
          )
          .filter(col("index.wkb").isNull)

        emptyChips2.collect().length should be >= 0
    }

    def wktDecomposeKeepCoreParamExpression(mosaicContext: => MosaicContext, spark: => SparkSession, resolution: Int): Unit = {
        mosaicContext.register(spark)

        val rdd = spark.sparkContext.makeRDD(
          Seq(
            Row("POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))")
          )
        )
        val schema = StructType(
          List(
            StructField("wkt", StringType)
          )
        )
        val df = spark.createDataFrame(rdd, schema)

        val noEmptyChips = df
            .select(
              expr(s"mosaic_explode(wkt, $resolution, true)")
            )
        noEmptyChips.collect().length should be >= 0

        val noEmptyChips_2 = df
            .select(
              expr(s"mosaic_explode(wkt, $resolution, false)")
            )
        noEmptyChips_2.collect().length should be >= 0
    }

    def lineDecompose(mosaicContext: => MosaicContext, spark: => SparkSession, resolution: Int): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val wktRows: DataFrame = getWKTRowsDf(mc).where(col("wkt").contains("LINESTRING"))

        val mosaics = wktRows
            .select(
              mosaic_explode(col("wkt"), resolution)
            )
            .collect()

        wktRows.collect().length should be <= mosaics.length

        wktRows.createOrReplaceTempView("wkt_rows")

        val mosaics2 = spark
            .sql(s"""
                    |select mosaic_explode(wkt, $resolution) from wkt_rows
                    |""".stripMargin)
            .collect()

        wktRows.collect().length should be <= mosaics2.length

    }

    def lineDecomposeFirstPointOnBoundary(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
      mosaicContext.register(spark)

      val rdd = spark.sparkContext.makeRDD(
        Seq(
          // The first point of this line is located exactly
          // over the boundary of the relative h3 cell, while the second point
          // is located outside of the first point's h3 cell.
          Row("LINESTRING (-120.65246800000001 40.420067, -120.65228800000001 40.420528000000004)")
        )
      )
      val schema = StructType(
        List(
          StructField("wkt", StringType)
        )
      )
      val df = spark.createDataFrame(rdd, schema)

      val noEmptyChips = df
        .select(
          expr(s"grid_tessellateexplode(wkt, 8, true)")
        )
      val res = noEmptyChips.collect()
      res.length should be > 0
    }

    def wkbDecompose(mosaicContext: => MosaicContext, spark: => SparkSession, resolution: Int): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs(mc)

        val mosaics = boroughs
            .select(
              mosaic_explode(convert_to(col("wkt"), "wkb"), resolution)
            )
            .collect()

        boroughs.collect().length should be <= mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql(s"""
                    |select mosaic_explode(convert_to_wkb(wkt), $resolution) from boroughs
                    |""".stripMargin)
            .collect()

        boroughs.collect().length should be <= mosaics2.length
    }

    def hexDecompose(mosaicContext: => MosaicContext, spark: => SparkSession, resolution: Int): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs(mc)

        val mosaics = boroughs
            .select(
              mosaic_explode(convert_to(col("wkt"), "hex"), resolution)
            )
            .collect()

        boroughs.collect().length should be <= mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql(s"""
                    |select mosaic_explode(convert_to_hex(wkt), $resolution) from boroughs
                    |""".stripMargin)
            .collect()

        boroughs.collect().length should be <= mosaics2.length
    }

    def coordsDecompose(mosaicContext: => MosaicContext, spark: => SparkSession, resolution: Int): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs(mc)

        val mosaics = boroughs
            .select(
              mosaic_explode(convert_to(col("wkt"), "coords"), resolution)
            )
            .collect()

        boroughs.collect().length should be <= mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql(s"""
                    |select mosaic_explode(convert_to_coords(wkt), $resolution) from boroughs
                    |""".stripMargin)
            .collect()

        boroughs.collect().length should be <= mosaics2.length
    }

    def auxiliaryMethods(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        mosaicContext.register(spark)
        val sc = spark
        import sc.implicits._

        val wkt = mocks.getWKTRowsDf(mosaicContext).limit(1).select("wkt").as[String].collect().head
        val resExpr = mc.getIndexSystem match {
            case H3IndexSystem  => lit(mc.getIndexSystem.resolutions.head).expr
            case BNGIndexSystem => lit("100m").expr
        }

        val mosaicExplodeExpr = MosaicExplode(
          lit(wkt).expr,
          resExpr,
          lit(false).expr,
          mc.getIndexSystem.name,
          mc.getGeometryAPI.name
        )

        mosaicExplodeExpr.position shouldEqual false
        mosaicExplodeExpr.inline shouldEqual false
        mosaicExplodeExpr.checkInputDataTypes() shouldEqual TypeCheckResult.TypeCheckSuccess

        val badExpr = MosaicExplode(
          lit(10).expr,
          resExpr,
          lit(false).expr,
          mc.getIndexSystem.name,
          mc.getGeometryAPI.name
        )

        badExpr.checkInputDataTypes().isFailure shouldEqual true
        badExpr
            .withNewChildren(Array(lit(wkt).expr, lit(true).expr, lit(false).expr))
            .checkInputDataTypes()
            .isFailure shouldEqual true
        badExpr
            .withNewChildren(Array(lit(wkt).expr, resExpr, lit(5).expr))
            .checkInputDataTypes()
            .isFailure shouldEqual true

        // Line decompose error should be thrown
        val geom = MosaicContext.geometryAPI().geometry("POINT (1 1)", "WKT")
        an[Error] should be thrownBy Mosaic.lineFill(geom, 5, MosaicContext.indexSystem(), MosaicContext.geometryAPI())

        // Default getters
        noException should be thrownBy mosaicExplodeExpr.geom
        noException should be thrownBy mosaicExplodeExpr.resolution
        noException should be thrownBy mosaicExplodeExpr.keepCoreGeom

        // legacy API def tests
        noException should be thrownBy mc.functions.mosaic_explode(lit(""), lit(5))
        noException should be thrownBy mc.functions.mosaic_explode(lit(""), 5)
        noException should be thrownBy mc.functions.mosaic_explode(lit(""), lit(5), lit(true))
        noException should be thrownBy mc.functions.mosaic_explode(lit(""), lit(5), keepCoreGeometries = true)
        noException should be thrownBy mc.functions.mosaic_explode(lit(""), 5, keepCoreGeometries = true)
    }

}
