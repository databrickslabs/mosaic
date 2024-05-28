package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.scalatest.matchers.should.Matchers._

trait RST_ClipBehaviors extends QueryTest {

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

        val gridTiles = rastersInMemory
            .withColumn("bbox", rst_boundingbox($"tile"))
            .withColumn("cent", st_centroid($"bbox"))
            .withColumn("clip_region", st_buffer($"cent", 0.1))
            .withColumn("clip", rst_clip($"tile", $"clip_region"))
            .withColumn("bbox2", rst_boundingbox($"clip"))
            .withColumn("result", st_area($"bbox") =!= st_area($"bbox2"))
            .select("result")
            .as[Boolean]
            .collect()

        gridTiles.forall(identity) should be(true)

        rastersInMemory.createOrReplaceTempView("source")

        noException should be thrownBy spark
            .sql("""
                   |select
                   |  rst_clip(tile, st_buffer(st_centroid(rst_boundingbox(tile)), 0.1)) as tile
                   |from source
                   |""".stripMargin)
            .collect()

    }

}
