package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.{MOSAIC_MANUAL_CLEANUP_MODE, MOSAIC_RASTER_LOCAL_AGE_LIMIT_DEFAULT, MOSAIC_RASTER_LOCAL_AGE_LIMIT_MINUTES, MOSAIC_RASTER_USE_CHECKPOINT, MOSAIC_RASTER_USE_CHECKPOINT_DEFAULT, MOSAIC_TEST_MODE}
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.io.CleanUpManager
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.gdal.MosaicGDAL
import com.databricks.labs.mosaic.utils.FileUtils
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.BinaryType
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Paths
import scala.collection.mutable
import scala.util.Try

trait RST_ClipBehaviors extends QueryTest {

    // noinspection MapGetGet
    def behaviors(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        val sc = this.spark
        import sc.implicits._

        //        sc.conf.set(MOSAIC_MANUAL_CLEANUP_MODE, "true")
        //        sc.conf.set(MOSAIC_RASTER_USE_CHECKPOINT, "true")

        // init
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register(sc)
        import mc.functions._

        info(s"is CleanUpManager running? ${CleanUpManager.isCleanThreadAlive}")

        info(s"test on? ${sc.conf.get(MOSAIC_TEST_MODE, "false")}")
        info(s"manual cleanup on? ${sc.conf.get(MOSAIC_MANUAL_CLEANUP_MODE, "false")}")
        info(s"cleanup minutes (config)? ${sc.conf.get(MOSAIC_RASTER_LOCAL_AGE_LIMIT_MINUTES, MOSAIC_RASTER_LOCAL_AGE_LIMIT_DEFAULT)}")

//        val checkDir = MosaicGDAL.getCheckpointPath
//        info(s"configured checkpoint dir? $checkDir")
//        info(s"checkpoint on? ${sc.conf.get(MOSAIC_RASTER_USE_CHECKPOINT, MOSAIC_RASTER_USE_CHECKPOINT_DEFAULT)}")
//
//        val localDir = MosaicGDAL.getLocalRasterDir
//        info(s"configured local raster dir? $localDir")
//        info(s"local dir exists and is dir? -> ${Paths.get(localDir).toFile.exists()} |" +
//            s" ${Paths.get(localDir).toFile.isDirectory}")
//        info(s"last modified for working dir? -> ${Paths.get(localDir).toFile.lastModified()}")
//        info(s"current time millis -> ${System.currentTimeMillis()}")

//        // clean up configured MosaicTmpRootDir
//        // - all but those in the last 5 minutes
//        GDAL.cleanUpManualDir(ageMinutes = 5, MosaicGDAL.getMosaicTmpRootDir, keepRoot = true) match {
//            case Some(msg) => info(s"cleanup mosaic tmp dir msg -> '$msg'")
//            case _ => ()
//        }

        val testPath = "src/test/resources/binary/geotiff-small/chicago_sp27.tif"

        info("\n::: base :::")
        val df = spark.read.format("gdal").load(testPath)
            .withColumn("content", $"tile.raster")
            .withColumn("pixels", rst_pixelcount($"tile"))
            .withColumn("size", rst_memsize($"tile"))
            .withColumn("srid", rst_srid($"tile"))
            .withColumn("pixel_height", rst_pixelheight($"tile"))
            .withColumn("pixel_width", rst_pixelwidth($"tile"))
            .select("pixels", "srid", "size", "tile", "pixel_height", "pixel_width", "content")
            .limit(1)

        val base = df.first
        val p = base.getAs[mutable.WrappedArray[Long]](0)(0)
        val srid = base.get(1).asInstanceOf[Int]
        val sz = base.get(2)
        val tile = base.get(3)
        val ph = base.get(4).asInstanceOf[Double]
        val pw = base.get(5).asInstanceOf[Double]
//        val content = base.get(6)
        info(s"tile -> $tile (${tile.getClass.getName})")
        info(s"size -> $sz")
        info(s"pixels -> $p")
        info(s"srid -> $srid (${srid.getClass.getName})")
        info(s"pixel_height -> $ph")
        info(s"pixel_width -> $pw")

        info("\n::: clipper :::")
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
        info(s"buffVal -> $buffVal")
        info(s"clip-srid -> $clipSRID")
        clipSRID == 0 should be(true)

        val gRegion = geometryAPI.geometry(regionWKB, BinaryType)
        gRegion.setSpatialReference(srid)
        val wkbRegion4326 = gRegion.transformCRSXY(4326).toWKB

        info("\n::: clip tests :::")

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
        info(s"gH3Trans area -> ${gH3Trans.getArea}")
        val clipWKB = gH3Trans.toWKB

        val r1 = df
            .withColumn("clip", rst_clip($"tile", lit(clipWKB))) // <- touches
            .withColumn("pixels", rst_pixelcount($"clip"))
            .select("clip", "pixels")
            .first

        //        val c1 = r1.asInstanceOf[GenericRowWithSchema].get(0)
        //        val createInfo1 = c1.asInstanceOf[GenericRowWithSchema].getAs[Map[String, String]](2)
        //        val path1 = createInfo1("path")
        //        val sz1 = createInfo1("mem_size").toInt
        //        info(s"clip-touches -> $c1 (${c1.getClass.getName})")
        //        info(s"clip-touches-createInfo -> $createInfo1")
        //        info(s"...clip-touches-path -> $path1")
        //        info(s"...clip-touches-memsize -> $sz1}")
        //        Paths.get(path1).toFile.exists should be(true)

        val p1 = r1.getAs[mutable.WrappedArray[Long]](1)(0)
        info(s"clip-touches-pixels -> $p1")

        val r2 = df
            .withColumn("clip", rst_clip($"tile", lit(clipWKB), lit(false))) // <- half-in
            .withColumn("pixels", rst_pixelcount($"clip"))
            .select("clip", "pixels")
            .first

        //        val c2 = r2.asInstanceOf[GenericRowWithSchema].get(0)
        //        val createInfo2 = c2.asInstanceOf[GenericRowWithSchema].getAs[Map[String, String]](2)
        //        val path2 = createInfo2("path")
        //        val sz2 = createInfo2("mem_size").toInt
        //        //info(s"clip-half -> $c2 (${c2.getClass.getName})")
        //        //info(s"clip-half-createInfo -> $createInfo2")
        //        //info(s"...clip-half-path -> $path2")
        //        info(s"...clip-half-memsize -> $sz2}")
        //        Paths.get(path2).toFile.exists should be(true)

        val p2 = r2.getAs[mutable.WrappedArray[Long]](1)(0)
        info(s"clip-half-pixels -> $p2")

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
