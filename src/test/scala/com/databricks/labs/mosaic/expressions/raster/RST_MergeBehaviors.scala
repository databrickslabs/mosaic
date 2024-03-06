package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.collect_list
import org.scalatest.matchers.should.Matchers._

trait RST_MergeBehaviors extends QueryTest {

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
            .option("pathGlobFilter", "*_B01.TIF")
            .load("src/test/resources/modis")

        val gridTiles = rastersInMemory
            .withColumn("tile", rst_tessellate($"tile", 3))
            .select("path", "tile")
            .groupBy("path")
            .agg(
              collect_list("tile").as("tiles")
            )
            .select(
              rst_merge($"tiles").as("tile")
            )

        rastersInMemory
            .createOrReplaceTempView("source")

        spark.sql("""
                    |select rst_merge(tiles) as tile
                    |from (
                    |  select collect_list(tile) as tiles
                    |  from (
                    |    select path, rst_tessellate(tile, 3) as tile
                    |    from source
                    |  )
                    |  group by path
                    |)
                    |""".stripMargin)

        noException should be thrownBy rastersInMemory
            .withColumn("tile", rst_tessellate($"tile", 3))
            .select("path", "tile")
            .groupBy("path")
            .agg(
              collect_list("tile").as("tiles")
            )
            .select(
              rst_merge($"tiles").as("tile")
            )

        val result = gridTiles.collect()

        result.length should be(rastersInMemory.count())

    }

}
