package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions._
import org.scalatest.matchers.should.Matchers._

trait RST_RasterToGridCountBehaviors extends QueryTest {

    def rasterToGridCountBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register()
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val rastersInMemory = spark.read
            .format("gdal")
            .option("raster_storage", "in-memory")
            .load("src/test/resources/modis")

        val df = rastersInMemory
            .withColumn("result", rst_rastertogridcount($"tile", lit(3)))
            .select("result")
            .select(explode($"result").as("result"))
            .select(explode($"result").as("result"))
            .select($"result".getItem("measure").as("result"))

        rastersInMemory
            .createOrReplaceTempView("source")

        noException should be thrownBy spark.sql("""
                                                   |select rst_rastertogridcount(tile, 3) from source
                                                   |""".stripMargin)

        noException should be thrownBy rastersInMemory
            .withColumn("result", rst_rastertogridcount($"tile", lit(3)))
            .select("result")

        val result = df.as[Int].collect().max

        result should be > 0

        an[Exception] should be thrownBy spark.sql("""
                                                     |select rst_rastertogridcount() from source
                                                     |""".stripMargin)

    }

}
