package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.{MosaicSpatialQueryTest, mocks}
import com.databricks.labs.mosaic.utils.SysUtils
import org.apache.spark.sql.functions._
import org.gdal.ogr.ogr
import org.scalatest.matchers.must.Matchers.noException
import org.scalatest.matchers.should.Matchers.{be, convertToAnyShouldWrapper}

import java.nio.file.{Files, Paths}

trait ST_AsMVTTileAggBehaviors extends MosaicSpatialQueryTest {

    def behavior(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        import mc.functions._
        val sc = spark
        import sc.implicits._
        mc.register(spark)

        val srcSR = mc.getIndexSystem match {
            case H3IndexSystem  => 4326
            case BNGIndexSystem => 27700
            case _              => 4326
        }

        val result = mocks
            .getWKTRowsDf(mc.getIndexSystem)
            .select(st_centroid($"wkt").alias("centroid"))
            .withColumn("ids", array((0 until 30).map(_ => rand() * 1000): _*)) // add some random data
            .select(explode($"ids").alias("id"), st_translate($"centroid", rand(), rand()).alias("centroid"))
            .withColumn("index_id", grid_pointascellid($"centroid", lit(6)))
            .withColumn("centroid", as_json(st_asgeojson($"centroid")))
            .withColumn("centroid", st_updatesrid($"centroid", lit(srcSR), lit(3857)))
            .groupBy("index_id")
            .agg(st_asmvttile_agg($"centroid", struct($"id"), lit("5/21/9")).alias("mvt"))
            .collect()
        
        val row = result.head
        
        val payload = row.getAs[Array[Byte]]("mvt")
        
        
        val tmpFile = Files.createTempFile(Paths.get("/tmp"), "mvt", ".pbf")
        Files.write(tmpFile, payload)

        val ds = ogr.GetDriverByName("MVT").Open(tmpFile.toAbsolutePath.toString)
        
        ds.GetLayerCount should be(1L)
        ds.GetLayer(0).GetFeatureCount should be > 0L
    }

}
