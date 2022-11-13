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
trait CellKDiscBehaviors extends MosaicSpatialQueryTest {

    def behaviorComputedColumns(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = mosaicContext
        import mc.functions._
        mc.register(spark)

        val k = 3
        val resolution = 4

        val boroughs: DataFrame = getBoroughs(mc)
            .withColumn("centroid", st_centroid2D(col("wkt")))
            .withColumn("point", st_point(col("centroid.x"), col("centroid.y")))
            .withColumn("cell_id", grid_pointascellid(col("point"), resolution))

        val mosaics = boroughs
            .select(
              grid_cellkdisc(col("cell_id"), k)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length
    }

    def auxiliaryMethods(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val sc = spark
        import sc.implicits._
        val mc = mosaicContext
        mc.register(spark)

        val wkt = mocks.getWKTRowsDf(mc).limit(1).select("wkt").as[String].collect().head
        val k = 4

        val cellKDiscExpr = CellKDisc(
          lit(wkt).expr,
          lit(k).expr,
          mc.getIndexSystem.name,
          mc.getGeometryAPI.name
        )

        mc.getIndexSystem match {
            case H3IndexSystem  => cellKDiscExpr.dataType shouldEqual ArrayType(LongType)
            case BNGIndexSystem => cellKDiscExpr.dataType shouldEqual ArrayType(StringType)
        }

        val badExpr = CellKDisc(
          lit(10).expr,
          lit(true).expr,
          mc.getIndexSystem.name,
          mc.getGeometryAPI.name
        )

        an[Error] should be thrownBy badExpr.inputTypes

        noException should be thrownBy mc.functions.grid_cellkdisc(lit(""), lit(k))
        noException should be thrownBy mc.functions.grid_cellkdisc(lit(""), k)

        noException should be thrownBy cellKDiscExpr.makeCopy(cellKDiscExpr.children.toArray)
        noException should be thrownBy cellKDiscExpr.withNewChildrenInternal(Array(cellKDiscExpr.children: _*))
    }

}
