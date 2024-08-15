package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.{array, lit}
import org.scalatest.matchers.should.Matchers._

trait RST_FromBandsBehaviors extends QueryTest {

    // noinspection MapGetGet
    def behaviors(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        val sc = this.spark
        import sc.implicits._
        sc.sparkContext.setLogLevel("ERROR")

        // init
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register(sc)
        import mc.functions._

        val rastersDf = spark.read
            .format("gdal")
                .option("pathGlobFilter", "*.TIF")
            .load("src/test/resources/modis")

        val gridTiles = rastersDf
            .withColumn("bbox", rst_boundingbox($"tile"))
            .withColumn("tile1", rst_write($"tile", lit("/dbfs/checkpoint/mosaic_tmp/tile1")))
            .withColumn("tile2", rst_write($"tile", lit("/dbfs/checkpoint/mosaic_tmp/tile2")))
            .withColumn("tile3", rst_write($"tile", lit("/dbfs/checkpoint/mosaic_tmp/tile3")))
            .withColumn("stacked", rst_frombands(array($"tile1", $"tile2", $"tile3")))
            .withColumn("bbox2", rst_boundingbox($"stacked"))
            .withColumn("area", st_area($"bbox"))
            .withColumn("area2", st_area($"bbox2"))
            .withColumn("result", $"area" === $"area2")

        //info(gridTiles.select("area", "area2", "result", "stacked", "bbox", "bbox2").first().toString())
        //info(gridTiles.select("tile1").first().toString())
        val result = gridTiles
            .select("result")
            .as[Boolean]
            .collect()

        //info(result.toSeq.toString())

        result.forall(identity) should be(true)

        gridTiles
            .drop("bbox", "stacked", "bbox2", "area", "area2", "result")
            .createOrReplaceTempView("source")

        val gridTilesSQL = spark
            .sql("""
                   |with subquery (
                   |   select rst_frombands(array(tile1, tile2, tile3)) as stacked, tile from source
                   |)
                   |select st_area(rst_boundingbox(tile)) == st_area(rst_boundingbox(stacked)) as result
                   |from subquery
                   |""".stripMargin)
            .as[Boolean]
            .collect()

        gridTilesSQL.forall(identity) should be(true)

    }

}
