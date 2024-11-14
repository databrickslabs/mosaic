package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.{MOSAIC_RASTER_READ_IN_MEMORY, MOSAIC_RASTER_READ_STRATEGY}
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks.filePath
import org.apache.spark.sql.QueryTest
import org.scalatest.matchers.should.Matchers._

trait RST_FormatBehaviors extends QueryTest {

    def formatBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register()
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val rastersInMemory = spark.read
            .format("gdal")
            .option(MOSAIC_RASTER_READ_STRATEGY, MOSAIC_RASTER_READ_IN_MEMORY)
            .load(filePath("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF"))

        val df = rastersInMemory
            .withColumn("result", rst_format($"tile"))
            .select("result")

        rastersInMemory
            .createOrReplaceTempView("source")

        noException should be thrownBy spark.sql("""
                                                   |select rst_format(tile) from source
                                                   |""".stripMargin)

        noException should be thrownBy rastersInMemory
            .withColumn("result", rst_format($"tile"))
            .select("result")

        val result = df.first.getString(0)

        result shouldBe "GTiff"

        an[Exception] should be thrownBy spark.sql("""
                                                     |select rst_format() from source
                                                     |""".stripMargin)

    }

}
