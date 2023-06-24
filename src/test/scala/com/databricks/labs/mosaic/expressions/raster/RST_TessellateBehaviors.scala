package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.scalatest.matchers.should.Matchers._

trait RST_TessellateBehaviors extends QueryTest {

    // noinspection MapGetGet
    def tessellateBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register()
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val rastersAsPaths = spark.read
            .format("gdal")
            .option("raster_storage", "disk")
            .load("src/test/resources/modis")

        val rastersInMemory = spark.read
            .format("gdal")
            .option("raster_storage", "in-memory")
            .load("src/test/resources/modis")

        val gridTiles = rastersAsPaths
            .withColumn("tiles", rst_tessellate($"path", 3))
            .select("tiles")

        rastersInMemory
            .createOrReplaceTempView("source")

        noException should be thrownBy spark.sql("""
                                                   |select rst_tessellate(raster, 3) from source
                                                   |""".stripMargin)

        noException should be thrownBy rastersInMemory
            .withColumn("tiles", rst_tessellate($"raster", 3))
            .select("tiles")

        val result = gridTiles.collect()

        result.length should be(791)

    }

}
