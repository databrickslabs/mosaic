package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks.filePath
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.lit
import org.scalatest.matchers.should.Matchers._

trait RST_ClipBehaviors extends QueryTest {

    // noinspection MapGetGet
    def basicBehaviour(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
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

    def cutlineBehaviour(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register()
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val testPath = filePath("/binary/geotiff-small/chicago_sp27.tif")

        val createInfo = Map("path" -> testPath, "parentPath" -> testPath)
        val testRaster = MosaicRasterGDAL.readRaster(createInfo)

        val ftMeters = 0.3 // ~0.3 ft in meter
        val ftUnits = 0.3 // epsg:26771 0.3 ft per unit
        val buffVal: Double = testRaster.pixelXSize * ftMeters * ftUnits * 50.5
        val clipRegion = testRaster
            .bbox(geometryAPI, testRaster.getSpatialReference)
            .getCentroid
            .buffer(buffVal)

        clipRegion.getSpatialReference shouldBe 26771

        val df = spark.read.format("gdal").load(testPath)
            .withColumn("clipGeom", st_geomfromgeojson(lit(clipRegion.toJSON)))

        val df_include = df
            .withColumn("clippedRaster", rst_clip($"tile", $"clipGeom"))
            .select(rst_pixelcount($"clippedRaster").alias("pCount"))
        val df_exclude = df
            .withColumn("clippedRaster", rst_clip($"tile", $"clipGeom", lit(false)))
            .select(rst_pixelcount($"clippedRaster").alias("pCount"))

        val pCountInclude = df_include.first.getSeq[Long](0).head
        val pCountExclude = df_exclude.first.getSeq[Long](0).head

        pCountInclude should be > pCountExclude

    }

}
