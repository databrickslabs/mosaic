package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.{MOSAIC_RASTER_USE_CHECKPOINT, MOSAIC_TEST_MODE}
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions._
import org.scalatest.matchers.should.Matchers._

trait RST_MaxBehaviors extends QueryTest {

    def behavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        val sc = this.spark
        import sc.implicits._
        sc.conf.set(MOSAIC_RASTER_USE_CHECKPOINT, "false")

        sc.conf.get(MOSAIC_TEST_MODE, "false") should be("true")

        // init
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register(sc)
        import mc.functions._

        val rastersInMemory = spark.read
            .format("gdal")
            .option("pathGlobFilter", "*.TIF")
            .load("src/test/resources/modis")

        val df = rastersInMemory
            .withColumn("tile", rst_tessellate($"tile", lit(3)))
            .withColumn("result", rst_max($"tile"))
            .select("result")
            .select(explode($"result").as("result"))

        rastersInMemory
            .withColumn("tile", rst_tessellate($"tile", lit(3)))
            .createOrReplaceTempView("source")

        noException should be thrownBy spark.sql("""
                                                   |select rst_max(tile) from source
                                                   |""".stripMargin)

        val result = df.as[Double].collect().max

        result > 0 shouldBe true

        an[Exception] should be thrownBy spark.sql("""
                                                     |select rst_max() from source
                                                     |""".stripMargin)

    }

}
