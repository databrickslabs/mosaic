package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.lit
import org.scalatest.matchers.should.Matchers._

trait RST_FilterBehaviors extends QueryTest {

    // noinspection MapGetGet
    def behaviors(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register()
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val rastersInMemory = spark.read
            .format("gdal")
            .option("raster_storage", "in-memory")
            .load("src/test/resources/modis")

        val gridTiles = rastersInMemory
            .withColumn("result", rst_filter($"tile", 3, "mode"))
            .select("result")
            .collect()

        gridTiles.length should be(7)

        val gridTiles2 = rastersInMemory
            .withColumn("result", rst_filter($"tile", lit(3), lit("mode")))
            .select("result")
            .collect()

        gridTiles2.length should be(7)

        val gridTiles3 = rastersInMemory
            .withColumn("result", rst_filter($"tile", lit(3), lit("avg")))
            .select("result")
            .collect()

        gridTiles3.length should be(7)

        val gridTiles4 = rastersInMemory
            .withColumn("result", rst_filter($"tile", lit(3), lit("min")))
            .select("result")
            .collect()

        gridTiles4.length should be(7)

        val gridTiles5 = rastersInMemory
            .withColumn("result", rst_filter($"tile", lit(3), lit("max")))
            .select("result")
            .collect()

        gridTiles5.length should be(7)

        val gridTiles6 = rastersInMemory
            .withColumn("result", rst_filter($"tile", lit(3), lit("median")))
            .select("result")
            .collect()

        gridTiles6.length should be(7)

        rastersInMemory.createOrReplaceTempView("source")
        
        noException should be thrownBy spark
            .sql("""
                   |select rst_filter(tile, 3, 'mode') as tile from source
                   |""".stripMargin)
            .collect()

    }

}
