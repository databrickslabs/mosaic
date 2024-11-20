package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.scalatest.matchers.should.Matchers._

trait RST_FromContentBehaviors extends QueryTest {

    // noinspection MapGetGet
    def behaviors(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register()
        val sc = spark

        import mc.functions._
        import org.apache.spark.sql.functions._
        import sc.implicits._

        val rastersInMemory = spark.read
            .format("binaryFile")
            .load("src/test/resources/modis")

        val gridTiles = rastersInMemory
            .withColumn("tile", rst_fromcontent($"content", "GTiff"))
            .withColumn("bbox", rst_boundingbox($"tile"))
            .withColumn("cent", st_centroid($"bbox"))
            .withColumn("clip_region", st_buffer($"cent", 1000.0))
            .withColumn("clip", rst_clip($"tile", $"clip_region"))
            .withColumn("bbox2", rst_boundingbox($"clip"))
            .withColumn("result", st_area($"bbox") =!= st_area($"bbox2"))
            .select("result")
            .as[Boolean]
            .collect()

        gridTiles.forall(identity) should be(true)

        rastersInMemory.createOrReplaceTempView("source")

        val gridTilesSQL = spark
            .sql("""
                   |with subquery as (
                   |   select rst_fromcontent(content, 'GTiff') as tile from source
                   |)
                   |select st_area(rst_boundingbox(tile)) != st_area(rst_boundingbox(rst_clip(tile, st_buffer(st_centroid(rst_boundingbox(tile)), 1000.0)))) as result
                   |from subquery
                   |""".stripMargin)
            .as[Boolean]
            .collect()

        gridTilesSQL.forall(identity) should be(true)


        val gridTilesSQL2 = spark
            .sql(
                """
                  |with subquery as (
                  |   select rst_fromcontent(content, 'GTiff', 4) as tile from source
                  |)
                  |select st_area(rst_boundingbox(tile)) != st_area(rst_boundingbox(rst_clip(tile, st_buffer(st_centroid(rst_boundingbox(tile)), 1000.0)))) as result
                  |from subquery
                  |""".stripMargin)
            .as[Boolean]
            .collect()

        gridTilesSQL2.forall(identity) should be(true)

    }

}
