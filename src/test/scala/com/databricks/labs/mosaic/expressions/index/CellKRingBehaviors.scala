package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.{mocks, MosaicSpatialQueryTest}
import com.databricks.labs.mosaic.test.mocks.getBoroughs
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers._

//noinspection ScalaDeprecation
trait CellKRingBehaviors extends MosaicSpatialQueryTest {

    def behaviorComputedColumns(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        import mc.functions._
        mc.register(spark)

        val k = 3
        val resolution = 4

        val boroughs: DataFrame = getBoroughs(mc)
            .withColumn("centroid", st_centroid(col("wkt")))
            .withColumn("cell_id", grid_pointascellid(col("centroid"), resolution))

        val mosaics = boroughs
            .select(
              grid_cellkring(col("cell_id"), k)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length
    }

    def columnFunctionSignatures(mosaicContext: MosaicContext): Unit = {
        val funcs = mosaicContext.functions
        noException should be thrownBy funcs.grid_cellkring(col("wkt"), lit(3))
        noException should be thrownBy funcs.grid_cellkring(col("wkt"), 3)
    }

    def auxiliaryMethods(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val sc = spark
        import sc.implicits._
        val mc = mosaicContext
        mc.register(spark)

        val wkt = mocks.getWKTRowsDf(mc.getIndexSystem).limit(1).select("wkt").as[String].collect().head
        val k = 4

        val cellKRingExpr = CellKRing(
          lit(wkt).expr,
          lit(k).expr,
          mc.getIndexSystem,
          mc.getGeometryAPI.name
        )

        mc.getIndexSystem match {
            case H3IndexSystem  => cellKRingExpr.dataType shouldEqual ArrayType(LongType)
            case BNGIndexSystem => cellKRingExpr.dataType shouldEqual ArrayType(StringType)
            case _  => cellKRingExpr.dataType shouldEqual ArrayType(LongType)
        }

        val badExpr = CellKRing(
          lit(10).expr,
          lit(true).expr,
          mc.getIndexSystem,
          mc.getGeometryAPI.name
        )

        an[Error] should be thrownBy badExpr.inputTypes

        noException should be thrownBy mc.functions.grid_cellkring(lit(""), lit(k))
        noException should be thrownBy mc.functions.grid_cellkring(lit(""), k)

        noException should be thrownBy cellKRingExpr.makeCopy(cellKRingExpr.children.toArray)
    }

}
