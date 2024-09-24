package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.scalatest.matchers.should.Matchers._

trait RST_CombineAvgAggBehaviors extends QueryTest {

    // noinspection MapGetGet
    def behaviors(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
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

        val gridTiles = rastersInMemory.union(rastersInMemory)
            .withColumn("tiles", rst_tessellate($"tile", 2))
            .select("path", "tiles")
            .groupBy("path")
            .agg(
              rst_combineavg_agg($"tiles").as("tiles")
            )
            .select("tiles")

        rastersInMemory.union(rastersInMemory)
            .createOrReplaceTempView("source")

        spark.sql("""
                    |select rst_combineavg_agg(tiles) as tiles
                    |from (
                    |  select path, rst_tessellate(tile, 2) as tiles
                    |  from source
                    |)
                    |group by path
                    |""".stripMargin)

        noException should be thrownBy rastersInMemory
            .withColumn("tiles", rst_tessellate($"tile", 2))
            .select("path", "tiles")
            .groupBy("path")
            .agg(
                rst_combineavg_agg($"tiles").as("tiles")
            )
            .select("tiles")

        val result = gridTiles.collect()

        result.length should be(rastersInMemory.count())

    }

}
