package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.{MosaicSpatialQueryTest, mocks}
import org.apache.spark.sql.functions._
import org.gdal.ogr.ogr
import org.scalatest.matchers.must.Matchers.noException
import org.scalatest.matchers.should.Matchers.{be, convertToAnyShouldWrapper}

trait ST_AsGeoJSONTileAggBehaviors extends MosaicSpatialQueryTest {

    def behavior(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = mosaicContext
        import mc.functions._
        val sc = spark
        import sc.implicits._
        mc.register(spark)

        val result = mocks
            .getWKTRowsDf(mc.getIndexSystem)
            .select(st_centroid($"wkt").alias("centroid"))
            .withColumn("ids", array((0 until 30).map(_ => rand() * 1000): _*)) // add some random data
            .select(explode($"ids").alias("id"), st_translate($"centroid", rand(), rand()).alias("centroid"))
            .withColumn("index_id", grid_pointascellid($"centroid", lit(6)))
            .groupBy("index_id")
            .agg(st_asgeojsontile_agg($"centroid", struct($"id")).alias("geojson"))
            .collect()

        val row = result.head
        
        val payload = row.getAs[String]("geojson")
        
        val ds = ogr.GetDriverByName("GeoJSON").Open(payload)
        
        ds.GetLayerCount should be(1L)
        ds.GetLayer(0).GetFeatureCount should be > 0L
        
    }

}
