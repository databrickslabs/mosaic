package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.{MOSAIC_RASTER_READ_IN_MEMORY, MOSAIC_RASTER_READ_STRATEGY}
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.lit
import org.scalatest.matchers.should.Matchers._

trait RST_SetSRIDBehaviors extends QueryTest {

    def setSRIDBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register()
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val modisPath = this.getClass.getResource("/modis/").getPath

        val rastersInMemory = spark.read
            .format("gdal")
            .option(MOSAIC_RASTER_READ_STRATEGY, MOSAIC_RASTER_READ_IN_MEMORY)
            .option("pathGlobFilter", "*.TIF")
            .load(modisPath)

        val df = rastersInMemory
            .withColumn("result", rst_setsrid($"tile", lit(4326)))
            .select("result")

        rastersInMemory
            .createOrReplaceTempView("source")

        noException should be thrownBy spark.sql("""
                                                   |select rst_setsrid(tile, 4326) from source
                                                   |""".stripMargin)

        noException should be thrownBy rastersInMemory
            .withColumn("result", rst_setsrid($"tile", lit(4326)))
            .select("result")

        val result = df
            .where(rst_srid($"result") === lit(4326))
            .collect
            .length

        result > 0 shouldBe true

        an[Exception] should be thrownBy spark.sql("""
                                                     |select rst_setsrid() from source
                                                     |""".stripMargin)

    }

}
