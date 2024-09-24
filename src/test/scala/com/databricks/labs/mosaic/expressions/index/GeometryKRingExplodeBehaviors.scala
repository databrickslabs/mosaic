package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.index.H3IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.{mocks, MosaicSpatialQueryTest}
import com.databricks.labs.mosaic.test.mocks.getBoroughs
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.matchers.should.Matchers._

//noinspection ScalaDeprecation
trait GeometryKRingExplodeBehaviors extends MosaicSpatialQueryTest {

    def behavior(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        import mc.functions._
        mc.register(spark)

        val k = 3
        val resolution = 4

        val boroughs: DataFrame = getBoroughs(mc)

        val mosaics = boroughs
            .select(
              col("id"),
              grid_geometrykringexplode(col("wkt"), resolution, k).alias("kring")
            )
            .groupBy(col("id"))
            .agg(collect_set("kring"))
            .collect()

        boroughs.collect().length shouldEqual mosaics.length
    }

    def columnFunctionSignatures(mosaicContext: MosaicContext): Unit = {
        val funcs = mosaicContext.functions
        noException should be thrownBy funcs.grid_geometrykringexplode(col("wkt"), lit(3), lit(3))
        noException should be thrownBy funcs.grid_geometrykringexplode(col("wkt"), lit(3), 3)
        noException should be thrownBy funcs.grid_geometrykringexplode(col("wkt"), 3, lit(3))
        noException should be thrownBy funcs.grid_geometrykringexplode(col("wkt"), 3, 3)
        noException should be thrownBy funcs.grid_geometrykringexplode(col("wkt"), "3", lit(3))
        noException should be thrownBy funcs.grid_geometrykringexplode(col("wkt"), "3", 3)
    }

    def auxiliaryMethods(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        mc.register(spark)
        val sc = spark
        import sc.implicits._

        val wkt = mocks.getWKTRowsDf(mc.getIndexSystem).limit(1).select("wkt").as[String].collect().head
        val k = 4
        val resolution = 3

        val geomKRingExplodeExpr = GeometryKRingExplode(
          lit(wkt).expr,
          lit(resolution).expr,
          lit(k).expr,
          mc.getIndexSystem,
          mc.getGeometryAPI.name
        )
        val withNull = geomKRingExplodeExpr.copy(geom = lit(null).expr)

        geomKRingExplodeExpr.position shouldEqual false
        geomKRingExplodeExpr.inline shouldEqual false
        geomKRingExplodeExpr.checkInputDataTypes() shouldEqual TypeCheckResult.TypeCheckSuccess
        withNull.eval(InternalRow.fromSeq(Seq(null, null, null))) shouldEqual Seq.empty

        val badExpr = GeometryKRingExplode(
          lit(10).expr,
          lit(10).expr,
          lit(k).expr,
          mc.getIndexSystem,
          mc.getGeometryAPI.name
        )

        badExpr.checkInputDataTypes().isFailure shouldEqual true
        badExpr
            .withNewChildren(Array(lit(wkt).expr, lit(true).expr, lit(true).expr))
            .checkInputDataTypes()
            .isFailure shouldEqual true
        geomKRingExplodeExpr
            .copy(k = lit(true).expr)
            .checkInputDataTypes()
            .isFailure shouldEqual true


        // Default getters
        noException should be thrownBy geomKRingExplodeExpr.geom
        noException should be thrownBy geomKRingExplodeExpr.resolution
        noException should be thrownBy geomKRingExplodeExpr.k

        noException should be thrownBy mc.functions.grid_geometrykringexplode(lit(""), lit(5), lit(2))
        noException should be thrownBy mc.functions.grid_geometrykringexplode(lit(""), lit(5), 2)
        noException should be thrownBy mc.functions.grid_geometrykringexplode(lit(""), 5, lit(2))
        noException should be thrownBy mc.functions.grid_geometrykringexplode(lit(""), 5, 2)
    }

    def issue360(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        import mc.functions._
        mc.register(spark)

        mc.getIndexSystem match {
            case H3IndexSystem =>
                val tests = Seq(
                    ("LINESTRING (-83.488541 42.220045, -83.4924813 42.2198315)", 13, 260),
                    ("LINESTRING (-83.4924813 42.2198315, -83.488541 42.220045)", 13, 260),
                    ("LINESTRING (-83.4924813 42.2198315, -83.488541 42.220045)", 12, 114),
                    ("LINESTRING (-86.4012318 41.671962, -86.4045068 41.6719684)", 13, 235),
                    ("LINESTRING (-86.4045068 41.6719684, -86.4012318 41.671962)", 13, 235),
                    ("LINESTRING (-85.0040681 42.2975028, -85.0073029 42.2975266)", 13, 224)
                )
                val schema = StructType(
                    List(
                        StructField("wkt", StringType)
                    )
                )
                val res: Seq[Boolean] = tests.map(
                    v => {
                        val rdd = spark.sparkContext.makeRDD(Seq(Row(v._1)))
                        val df = spark.createDataFrame(rdd, schema)
                        df.select(grid_geometrykringexplode(col("wkt"), v._2, 2))
                            .collect().length == v._3
                    }
                )
                res.reduce(_ && _) should be(true)

            case _ => // do nothing
        }
    }

}
