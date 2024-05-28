package com.databricks.labs.mosaic.models.knn

import com.databricks.labs.mosaic.core.geometry.api.JTS
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.{MOSAIC_GEOMETRY_API, MOSAIC_INDEX_SYSTEM, MOSAIC_RASTER_API}
import com.databricks.labs.mosaic.sql.extensions.SQLExtensionsBehaviors
import com.databricks.labs.mosaic.test.{MosaicSpatialQueryTest, SparkSuite}
import org.apache.spark.SparkConf
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.flatspec.AnyFlatSpec

class SpatialKNNTest extends AnyFlatSpec with SpatialKNNBehaviors with SparkSuite {

    "Mosaic" should "run SpatialKNN without approximation" in {
        var conf = new SparkConf(false)
            .set(MOSAIC_INDEX_SYSTEM, "H3")
            .set(MOSAIC_GEOMETRY_API, "JTS")
            .set(MOSAIC_RASTER_API, "GDAL")
            .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .set("spark.sql.parquet.compression.codec", "uncompressed")
        var spark = withConf(conf)
        spark.sparkContext.setLogLevel("ERROR")
        it should behave like noApproximation(MosaicContext.build(H3IndexSystem, JTS), spark)

        conf = new SparkConf(false)
            .set(MOSAIC_INDEX_SYSTEM, "BNG")
            .set(MOSAIC_GEOMETRY_API, "JTS")
            .set(MOSAIC_RASTER_API, "GDAL")
            .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .set("spark.sql.parquet.compression.codec", "uncompressed")
        spark = withConf(conf)
        spark.sparkContext.setLogLevel("ERROR")
        it should behave like noApproximation(MosaicContext.build(BNGIndexSystem, JTS), spark)

    }

    //testAllCodegen("SpatialKNN behavior with approximation") { behaviorApproximate }

}
