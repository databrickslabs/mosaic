package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.scalatest.matchers.should.Matchers._

trait RST_BoundingBoxBehaviors extends QueryTest {

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
            .select(st_area($"bbox").as("area"))
            .as[Double]
            .collect()

        gridTiles.forall(_ > 0.0) should be(true)

        rastersInMemory.createOrReplaceTempView("source")

        val gridTilesSQL = spark
            .sql("""
                   |SELECT ST_Area(RST_BoundingBox(tile)) AS area
                   |FROM source
                   |""".stripMargin)
            .as[Double]
            .collect()

        gridTilesSQL.forall(_ > 0.0) should be(true)

    }

}
