package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions._
import org.scalatest.matchers.should.Matchers._

trait RST_BandMetadataBehaviors extends QueryTest {

    def bandMetadataBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        val sc = spark
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register()

        import mc.functions._
        import sc.implicits._

        noException should be thrownBy mc.getRasterAPI
        noException should be thrownBy MosaicContext.geometryAPI

        val rastersAsPaths = spark.read
            .format("gdal")
            .option("raster_storage", "disk")
            .load("src/test/resources/binary/netcdf-coral")

        val rastersInMemory = spark.read
            .format("gdal")
            .option("raster_storage", "in-memory")
            .load("src/test/resources/binary/netcdf-coral")

        val rasterDfWithBandMetadata = rastersAsPaths
            .withColumn("subdatasets", rst_subdatasets($"path"))
            .withColumn("bleachingSubdataset", element_at($"subdatasets", "bleaching_alert_area"))
            .select(
              rst_bandmetadata($"bleachingSubdataset", lit(1))
                  .alias("metadata")
            )

        rastersInMemory
            .withColumn("subdatasets", rst_subdatasets($"path"))
            .withColumn("bleachingSubdataset", element_at($"subdatasets", "bleaching_alert_area"))
            .createOrReplaceTempView("source")

        noException should be thrownBy spark.sql("""
                                                   |select rst_bandmetadata(bleachingSubdataset, 1) from source
                                                   |""".stripMargin)

        noException should be thrownBy rastersInMemory
            .withColumn("subdatasets", rst_subdatasets($"raster"))
            .withColumn("bleachingSubdataset", element_at($"subdatasets", "bleaching_alert_area"))
            .select(
              rst_bandmetadata($"bleachingSubdataset", lit(1))
                  .alias("metadata")
            )
            .collect()

        val result = rasterDfWithBandMetadata.as[Map[String, String]].collect()

        result.head.keys.toList.contains("long_name") shouldBe true

        an[Exception] should be thrownBy spark.sql("""
                                                     |select rst_bandmetadata() from source
                                                     |""".stripMargin)

        noException should be thrownBy rst_bandmetadata($"bleachingSubdataset", lit(1))
        noException should be thrownBy rst_bandmetadata($"bleachingSubdataset", 1)
        noException should be thrownBy rst_bandmetadata("bleachingSubdataset", 1)

    }

}
