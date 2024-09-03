package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.{MOSAIC_RASTER_READ_IN_MEMORY, MOSAIC_RASTER_READ_STRATEGY}
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.{ExprConfig, MosaicContext}
import com.databricks.labs.mosaic.test.RasterTestHelpers
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.BinaryType
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Paths
import scala.collection.mutable

trait RST_ClipBehaviors extends QueryTest {

    // noinspection MapGetGet
    def behaviors(indexSystem: IndexSystem, geometryAPI: GeometryAPI, exprConfigOpt: Option[ExprConfig]): Unit = {
        val sc = this.spark
        import sc.implicits._

        // init
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register(sc)
        import mc.functions._

        val testPath = "src/test/resources/binary/geotiff-small/chicago_sp27.tif"

        //info("\n::: base :::")
        val df = spark.read.format("gdal")
            .option(MOSAIC_RASTER_READ_STRATEGY, MOSAIC_RASTER_READ_IN_MEMORY)
            .load(testPath)
            .withColumn("content", $"tile.raster")
            .withColumn("pixels", rst_pixelcount($"tile"))
            .withColumn("size", rst_memsize($"tile"))
            .withColumn("srid", rst_srid($"tile"))
            .withColumn("pixel_height", rst_pixelheight($"tile"))
            .withColumn("pixel_width", rst_pixelwidth($"tile"))
            .select("pixels", "srid", "size", "tile", "pixel_height", "pixel_width", "content")
            .limit(1)

        val tileBase = RasterTestHelpers.getFirstTile(df, exprConfigOpt)
        //info(s"tileBase (class? ${tileBase.getClass.getName}) -> $tileBase")
        tileBase.raster.isEmptyRasterGDAL should be(false)

        val base = df.first
        val p = base.getAs[mutable.WrappedArray[Long]](0)(0)
        val srid = base.get(1).asInstanceOf[Int]
        val sz = base.get(2)
        val ph = base.get(4).asInstanceOf[Double]
        val pw = base.get(5).asInstanceOf[Double]
        //info(s"size -> $sz")
        //info(s"srid -> $srid (${srid.getClass.getName})")
        //info(s"pixels -> $p")
        //info(s"pixel_height -> $ph")
        //info(s"pixel_width -> $pw")

        //info("\n::: clipper :::")
        val ftMeters = 0.3 // ~0.3 ft in meter
        val ftUnits = 0.3  // epsg:26771 0.3 ft per unit
        val buffVal: Double = ph * ftMeters * ftUnits * 50.5
        // trigger half-in policy dif
        val clipper = df
            .withColumn("bbox", rst_boundingbox($"tile"))
            .withColumn("cent", st_centroid($"bbox"))
            .withColumn("clip_region", st_buffer($"cent", buffVal))
            .withColumn("srid", st_srid($"clip_region"))
            .select("clip_region", "srid")
            .first
        val regionWKB = clipper.get(0)
        val clipSRID = clipper.get(1)
        //info(s"buffVal -> $buffVal")
        //info(s"clip-srid -> $clipSRID")
        clipSRID == 0 should be(true)

        val gRegion = geometryAPI.geometry(regionWKB, BinaryType)
        gRegion.setSpatialReference(srid)
        val wkbRegion4326 = gRegion.transformCRSXY(4326).toWKB

        //info("\n::: clip tests :::")
        // WKB that will produce same pixel outputs
        val h3WKB = {
            List(wkbRegion4326).toDF("wkb")
                .withColumn("centroid", st_centroid($"wkb"))
                .withColumn(
                    "cellid",
                    grid_longlatascellid(st_x($"centroid"), st_y($"centroid"), lit(12))
                )
                .select(grid_boundaryaswkb($"cellid"))
                .first.get(0)
        }
        val gH3 = geometryAPI.geometry(h3WKB, BinaryType)
        gH3.setSpatialReference(4326)
        val gH3Trans = gH3.transformCRSXY(srid)
        //info(s"gH3Trans area -> ${gH3Trans.getArea}")
        val clipWKB = gH3Trans.toWKB

        val df1= df
            .withColumn("clip", rst_clip($"tile", lit(clipWKB))) // <- touches
            .withColumn("pixels", rst_pixelcount($"clip"))
            .select("clip", "pixels")

        val r1Tile = RasterTestHelpers.getFirstTile(df1, exprConfigOpt, tileCol = "clip")
        //info(s"r1Tile (class? ${r1Tile.getClass.getName}) -> $r1Tile")
        r1Tile.raster.isEmptyRasterGDAL should be(false)
        Paths.get(r1Tile.raster.getPathGDAL.asFileSystemPath).toFile.exists should be(true)

        val r1 = df1.first
        val p1 = r1.getAs[mutable.WrappedArray[Long]](1)(0)
        //info(s"clip-touches-pixels -> $p1")

        val df2 = df
            .withColumn("clip", rst_clip($"tile", lit(clipWKB), lit(false))) // <- half-in
            .withColumn("pixels", rst_pixelcount($"clip"))
            .select("clip", "pixels")

        val r2Tile = RasterTestHelpers.getFirstTile(df2, exprConfigOpt, tileCol = "clip")
        //info(s"r2Tile (class? ${r2Tile.getClass.getName}) -> $r1Tile")
        r2Tile.raster.isEmptyRasterGDAL should be(false)
        Paths.get(r2Tile.raster.getPathGDAL.asFileSystemPath).toFile.exists should be(true)

        val r2 =  df2.first
        val p2 = r2.getAs[mutable.WrappedArray[Long]](1)(0)
        //info(s"clip-half-pixels -> $p2")

        p == p1 should be(false)
        p == p2 should be(false)
        p1 == p2 should be(false)

        df.createOrReplaceTempView("source")

        noException should be thrownBy spark
            .sql(
                """
                  |select
                  |  rst_clip(tile, st_buffer(st_centroid(rst_boundingbox(tile)), 0.1)) as tile
                  |from source
                  |""".stripMargin)
            .collect()
    }

}
