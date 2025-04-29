package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.lit
import org.scalatest.matchers.should.Matchers._

trait RST_HeightBehaviors extends QueryTest {

    def heightBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register()
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val rastersInMemory = spark.read
            .format("gdal")
            .option("raster_storage", "in-memory")
            .load("src/test/resources/binary/netcdf-CMIP5")

        val df = rastersInMemory
            .withColumn("result", rst_height($"tile"))
            .select("result")

        rastersInMemory
            .createOrReplaceTempView("source")

        noException should be thrownBy spark.sql("""
                                                   |select rst_height(tile) from source
                                                   |""".stripMargin)

        noException should be thrownBy rastersInMemory
            .withColumn("result", rst_height($"tile"))
            .select("result")

        val result = df.as[Int].collect()

        result.head should be > 0

        an[Exception] should be thrownBy spark.sql("""
                                                     |select rst_height() from source
                                                     |""".stripMargin)

    }

    def rstFromFileIntegrationBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register()
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val rasters = spark.read
            .format("binaryFile")
            .load("src/test/resources/modis/")

        val df = rasters
            .select("path")
            .withColumn("tile", rst_fromfile($"path", lit(50)))
            .repartition(20, $"path")
            .withColumn("result", rst_height($"tile"))
            .select("result")

        val result = df.as[Int].collect()

        result.head should be > 0

    }

}
