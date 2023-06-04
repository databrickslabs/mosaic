package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.scalatest.matchers.should.Matchers._

trait RST_MemSizeBehaviors extends QueryTest {

    def memSizeBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register()
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val rastersAsPaths = spark.read
            .format("gdal_binary")
            .option("raster_storage", "disk")
            .load("src/test/resources/binary/netcdf-coral")

        val rastersInMemory = spark.read
            .format("gdal_binary")
            .option("raster_storage", "in-memory")
            .load("src/test/resources/binary/netcdf-coral")

        val df = rastersAsPaths
            .withColumn("result", rst_memsize($"path"))
            .select("result")

        rastersInMemory
            .createOrReplaceTempView("source")

        noException should be thrownBy spark.sql("""
                                                   |select rst_memsize(content) from source
                                                   |""".stripMargin)

        noException should be thrownBy rastersInMemory
            .withColumn("result", rst_memsize($"content"))
            .select("result")

        val result = df.as[Long].collect()

        result.head should be > 0L

        an[Exception] should be thrownBy spark.sql("""
                                                     |select rst_memsize() from source
                                                     |""".stripMargin)

    }

}
