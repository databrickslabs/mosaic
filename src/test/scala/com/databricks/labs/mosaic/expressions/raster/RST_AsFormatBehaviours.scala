package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks.filePath
import com.databricks.labs.mosaic.{MOSAIC_RASTER_READ_IN_MEMORY, MOSAIC_RASTER_READ_STRATEGY}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.lit
import org.scalatest.matchers.should.Matchers._

trait RST_AsFormatBehaviours extends QueryTest {

    // noinspection MapGetGet
    def behavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register()
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val subDataset = "t2m"

        val rastersInMemory = spark.read
            .format("gdal")
            .option(MOSAIC_RASTER_READ_STRATEGY, MOSAIC_RASTER_READ_IN_MEMORY)
            .load(filePath("/binary/netcdf-ECMWF/"))
            .withColumn("tile", rst_getsubdataset($"tile", lit(subDataset)))

        val newFormat = "GTiff"

        val df = rastersInMemory
            .withColumn("updated_tile", rst_asformat($"tile", lit(newFormat)))
            .select(rst_format($"updated_tile").as("new_type"))

        rastersInMemory
            .createOrReplaceTempView("source")

        noException should be thrownBy spark.sql(s"""
                                                    |select rst_asformat(tile, '$newFormat') from source
                                                    |""".stripMargin)

        noException should be thrownBy rastersInMemory
            .withColumn("tile", rst_updatetype($"tile", lit(newFormat)))
            .select("tile")

        val result = df.first.getString(0)

        result shouldBe newFormat

    }

}