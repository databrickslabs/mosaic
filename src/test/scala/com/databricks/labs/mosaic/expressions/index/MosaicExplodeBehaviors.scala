package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.core.Mosaic
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.{mocks, MosaicSpatialQueryTest}
import com.databricks.labs.mosaic.test.mocks.{getBoroughs, getWKTRowsDf}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers._

//noinspection ScalaDeprecation
trait MosaicExplodeBehaviors extends MosaicSpatialQueryTest {

    def wktDecompose(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        import mc.functions._
        mc.register(spark)

        val resolution = mc.getIndexSystem match {
            case H3IndexSystem  => 3
            case BNGIndexSystem => 5
            case _              => 3
        }

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

    def wktDecomposeNoNulls(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        import mc.functions._
        mc.register(spark)

        val resolution = mc.getIndexSystem match {
            case H3IndexSystem  => 3
            case BNGIndexSystem => 5
            case _              => 3
        }

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

    def wktDecomposeKeepCoreParamExpression(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        mc.register(spark)

        val resolution = mc.getIndexSystem match {
            case H3IndexSystem  => 3
            case BNGIndexSystem => 5
            case _              => 3
        }

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

    def lineDecompose(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        import mc.functions._
        mc.register(spark)

        val resolution = mc.getIndexSystem match {
            case H3IndexSystem  => 3
            case BNGIndexSystem => 3
            case _              => 3
        }

        val wktRows: DataFrame = getWKTRowsDf(mc.getIndexSystem).where(col("wkt").contains("LINESTRING"))

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

    def lineDecomposeFirstPointOnBoundary(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        mc.register(spark)

        mc.getIndexSystem match {
            case H3IndexSystem =>
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
            case _             => // do nothing
        }

    }

    def wkbDecompose(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        import mc.functions._
        mc.register(spark)

        val resolution = mc.getIndexSystem match {
            case H3IndexSystem  => 3
            case BNGIndexSystem => 5
            case _              => 3
        }

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

    def hexDecompose(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        import mc.functions._
        mc.register(spark)

        val resolution = mc.getIndexSystem match {
            case H3IndexSystem  => 3
            case BNGIndexSystem => 5
            case _              => 3
        }

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

    def coordsDecompose(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        import mc.functions._
        mc.register(spark)

        val resolution = mc.getIndexSystem match {
            case H3IndexSystem  => 3
            case BNGIndexSystem => 5
            case _              => 3
        }

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

    def columnFunctionSignatures(mosaicContext: MosaicContext): Unit = {
        val funcs = mosaicContext.functions
        noException should be thrownBy funcs.grid_tessellateexplode(col("wkt"), lit(3))
        noException should be thrownBy funcs.grid_tessellateexplode(col("wkt"), 3)
        noException should be thrownBy funcs.grid_tessellateexplode(col("wkt"), 3, keepCoreGeometries = true)
        noException should be thrownBy funcs.grid_tessellateexplode(col("wkt"), 3, lit(false))
        noException should be thrownBy funcs.grid_tessellateexplode(col("wkt"), lit(3), lit(false))
        // legacy APIs
        noException should be thrownBy funcs.mosaic_explode(col("wkt"), 3)
        noException should be thrownBy funcs.mosaic_explode(col("wkt"), lit(3))
        noException should be thrownBy funcs.mosaic_explode(col("wkt"), 3, keepCoreGeometries = true)
        noException should be thrownBy funcs.mosaic_explode(col("wkt"), 3, lit(false))
        noException should be thrownBy funcs.mosaic_explode(col("wkt"), lit(3), lit(false))
    }

    def auxiliaryMethods(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        mc.register(spark)
        val sc = spark
        import sc.implicits._

        val wkt = mocks.getWKTRowsDf(mc.getIndexSystem).limit(1).select("wkt").as[String].collect().head
        val resExpr = mc.getIndexSystem match {
            case H3IndexSystem  => lit(mc.getIndexSystem.resolutions.head).expr
            case BNGIndexSystem => lit("100m").expr
            case _              => lit(3).expr
        }

        val mosaicExplodeExpr = MosaicExplode(
          lit(wkt).expr,
          resExpr,
          lit(false).expr,
          mc.getIndexSystem,
          mc.getGeometryAPI.name
        )

        mosaicExplodeExpr.position shouldEqual false
        mosaicExplodeExpr.inline shouldEqual false
        mosaicExplodeExpr.checkInputDataTypes() shouldEqual TypeCheckResult.TypeCheckSuccess

        val badExpr = MosaicExplode(
          lit(10).expr,
          resExpr,
          lit(false).expr,
          mc.getIndexSystem,
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
        val geom = MosaicContext.geometryAPI.geometry("POINT (1 1)", "WKT")
        an[Error] should be thrownBy Mosaic.lineFill(geom, 5, MosaicContext.indexSystem, MosaicContext.geometryAPI)

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

    def issue360(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        mc.register(spark)

        mc.getIndexSystem match {
            case H3IndexSystem =>
                val rdd = spark.sparkContext.makeRDD(
                  Seq(
                    Row("LINESTRING (-85.0040681 42.2975028, -85.0073029 42.2975266)")
                  )
                )
                val schema = StructType(
                  List(
                    StructField("wkt", StringType)
                  )
                )
                val df = spark.createDataFrame(rdd, schema)

                df.select(expr(s"grid_tessellateexplode(wkt, 12, true)"))
                    .collect()
                    .length shouldEqual 20

                df.select(expr(s"grid_tessellateexplode(wkt, 13, true)"))
                    .collect()
                    .length shouldEqual 48
            case _             => // do nothing
        }
    }

    def issue382(mosaicContext: MosaicContext): Unit = {
        assume(mosaicContext.getIndexSystem == H3IndexSystem)
        val sc = spark
        import sc.implicits._
        import mosaicContext.functions._

        val wkt = "POLYGON ((-8.522721910163417 53.40846416712235, -8.522828495418493 53.40871094834742," +
            " -8.523239522405696 53.40879676331252, -8.52334611088906 53.409043543609435," +
            " -8.523757142297253 53.409129356978674, -8.523863734008978 53.409376136347404," +
            " -8.523559290871438 53.40953710231036, -8.523665882370468 53.40978388071435," +
            " -8.523361436771772 53.40994484500841, -8.523468028058108 53.41019162244766," +
            " -8.523163579998224 53.410352585072815, -8.52275254184475 53.41026676959102," +
            " -8.52244809251643 53.41042772987954, -8.522037056535808 53.41034191209765," +
            " -8.521732605939153 53.41050287004956, -8.52132157213149 53.41041704996761," +
            " -8.521214991168797 53.410170272637956, -8.520803961782489 53.41008445096018," +
            " -8.520697384048132 53.40983767270238, -8.520286359083132 53.40975184942885," +
            " -8.520179784577046 53.409505070242936, -8.520484231594777 53.40934411429393," +
            " -8.52037765687575 53.409097334143304, -8.520682101432444 53.40893637652535," +
            " -8.520575526500501 53.408689595410024, -8.520879968596168 53.40852863612313," +
            " -8.521290986816735 53.408614457283946, -8.521595427644318 53.408453495660524," +
            " -8.522006448037782 53.40853931452139, -8.522310887597179 53.408378350561435," +
            " -8.522721910163417 53.40846416712235))"

        val rdd = spark.sparkContext.makeRDD(Seq(Row(wkt)))
        val schema = StructType(List(StructField("wkt", StringType)))
        val df = spark.createDataFrame(rdd, schema)

        val result = df
            .select(grid_tessellateexplode(col("wkt"), 11).alias("grid"))
            .select(col("grid.wkb"))
            .select(st_aswkt(col("wkb")))

        val chips = result.as[String].collect()
        val resultGeom = chips.map(mosaicContext.getGeometryAPI.geometry(_, "WKT"))
            .reduce(_ union _)

        val expected = mosaicContext.getGeometryAPI.geometry(wkt, "WKT")

      math.abs(expected.getArea - resultGeom.getArea) should be < 1e-8

    }

}
