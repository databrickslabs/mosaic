package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.MOSAIC_TEST
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.{array, lit}
import org.scalatest.matchers.should.Matchers._

trait RST_MapAlgebraBehaviors extends QueryTest {

    // noinspection MapGetGet
    def behaviors(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        spark.conf.set(MOSAIC_TEST, "true")
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
            .withColumn("tiles", array($"tile", $"tile", $"tile"))
            .withColumn("map_algebra", rst_mapalgebra($"tiles", lit("""{"calc": "A+B/C", "A_index": 0, "B_index": 1, "C_index": 2}""")))
            .select("tiles")

        rastersInMemory
            .createOrReplaceTempView("source")

        noException should be thrownBy spark.sql(
            """
              |select rst_mapalgebra(tiles, '{"calc": "A+B/C", "A_index": 0, "B_index": 1, "C_index": 2}')
              | as tiles
              |from (
              |  select array(tile, tile, tile) as tiles
              |  from source
              |)
              |""".stripMargin).take(1)

        noException should be thrownBy spark.sql(
            """
              |select rst_mapalgebra(tiles, '{"calc": "A+B/C"}')
              | as tiles
              |from (
              |  select array(tile, tile, tile) as tiles
              |  from source
              |)
              |""".stripMargin).take(1)

        noException should be thrownBy spark.sql(
            """
              |select rst_mapalgebra(tiles, '{"calc": "A+B/C", "A_index": 0, "B_index": 1, "C_index": 1}')
              | as tiles
              |from (
              |  select array(tile, tile, tile) as tiles
              |  from source
              |)
              |""".stripMargin).take(1)

        noException should be thrownBy spark.sql(
            """
              |select rst_mapalgebra(tiles, '{"calc": "A+B/C", "A_index": 0, "B_index": 1, "C_index": 2, "A_band": 1, "B_band": 1, "C_band": 1}')
              | as tiles
              |from (
              |  select array(tile, tile, tile) as tiles
              |  from source
              |)
              |""".stripMargin).take(1)

        val result = gridTiles.collect()

        result.length should be(rastersInMemory.count())

    }

}
