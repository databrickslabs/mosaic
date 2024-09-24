package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.array
import org.scalatest.matchers.should.Matchers._

trait RST_FromBandsBehaviors extends QueryTest {

    // noinspection MapGetGet
    def behaviors(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register()
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val rastersInMemory = spark.read
            .format("binaryFile")
            .load("src/test/resources/modis")

        val gridTiles = rastersInMemory
            .withColumn("tile", rst_fromfile($"path"))
            .withColumn("bbox", rst_boundingbox($"tile"))
            .withColumn("stacked", rst_frombands(array($"tile", $"tile", $"tile")))
            .withColumn("bbox2", rst_boundingbox($"stacked"))
            .withColumn("result", st_area($"bbox") === st_area($"bbox2"))
            .select("result")
            .as[Boolean]
            .collect()

        gridTiles.forall(identity) should be(true)

        rastersInMemory.createOrReplaceTempView("source")

        val gridTilesSQL = spark
            .sql("""
                   |with subquery as (
                   |   select rst_fromfile(path) as tile from source
                   |),
                   |subquery2 as (
                   |   select rst_frombands(array(tile, tile, tile)) as stacked, tile from subquery
                   |)
                   |select st_area(rst_boundingbox(tile)) == st_area(rst_boundingbox(stacked)) as result
                   |from subquery2
                   |""".stripMargin)
            .as[Boolean]
            .collect()

        gridTilesSQL.forall(identity) should be(true)

    }

}
