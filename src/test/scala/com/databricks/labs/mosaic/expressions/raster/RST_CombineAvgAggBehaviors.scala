package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.{array, explode, lit}
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
            .limit(2)

        val gridTiles = rastersInMemory
            .withColumn("tiles", rst_tessellate($"tile", 0))
            .select("path", "tiles")
            .withColumn("explode", explode(array(lit(1), lit(2))))
            .groupBy("path")
            .agg(
              rst_combineavg_agg($"tiles").as("tiles")
            )
            .select("tiles")

        rastersInMemory
            .withColumn("explode", explode(array(lit(1), lit(2))))
            .createOrReplaceTempView("source")

        spark.sql("""
                    |select rst_combineavg_agg(tiles) as tiles
                    |from (
                    |  select path, rst_tessellate(tile, 0) as tiles
                    |  from source
                    |)
                    |group by path
                    |""".stripMargin)

        noException should be thrownBy rastersInMemory
            .withColumn("tiles", rst_tessellate($"tile", 0))
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
