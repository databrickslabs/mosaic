package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.scalatest.matchers.should.Matchers._

trait RST_SubdatasetsBehaviors extends QueryTest {

    def subdatasetsBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register()
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val rastersAsPaths = spark.read
            .format("gdal")
            .option("raster_storage", "disk")
            .load("src/test/resources/binary/netcdf-coral")

        val rastersInMemory = spark.read
            .format("gdal")
            .option("raster_storage", "in-memory")
            .load("src/test/resources/binary/netcdf-coral")

        val rasterDfWithSubdatasets = rastersAsPaths
            .select(
              rst_subdatasets($"path")
                  .alias("subdatasets")
            )

        val result = rasterDfWithSubdatasets.as[Map[String, String]].collect()

        rastersAsPaths
            .orderBy("path")
            .createOrReplaceTempView("source")

        noException should be thrownBy spark.sql("""
                                                   |select rst_subdatasets(path) from source
                                                   |""".stripMargin)

        an[Exception] should be thrownBy spark.sql("""
                                                     |select rst_subdatasets() from source
                                                     |""".stripMargin)

        noException should be thrownBy spark.sql("""
                                                   |select rst_subdatasets("dummy/path") from source
                                                   |""".stripMargin)

        noException should be thrownBy rastersInMemory
            .select(
              rst_subdatasets($"content")
                  .alias("subdatasets")
            )
            .take(1)

        result.head.keys.toList.length shouldBe 6
        result.head.values.toList.map(_.nonEmpty).reduce(_ && _) shouldBe true

        noException should be thrownBy rst_subdatasets($"path")
        noException should be thrownBy rst_subdatasets("path")

    }

}
