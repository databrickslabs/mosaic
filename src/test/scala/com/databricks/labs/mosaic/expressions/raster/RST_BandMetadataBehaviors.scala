package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions._
import org.scalatest.matchers.should.Matchers._

trait RST_BandMetadataBehaviors extends QueryTest {

    def bandMetadataBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register()
        val sc = spark
        import mc.functions._
        import sc.implicits._

        noException should be thrownBy mc.getRasterAPI
        noException should be thrownBy MosaicContext.geometryAPI

        val rasterDfWithBandMetadata = mocks
            .getNetCDFBinaryDf(spark)
            .withColumn("subdatasets", rst_subdatasets($"path"))
            .withColumn("bleachingSubdataset", element_at($"subdatasets", "bleaching_alert_area"))
            .select(
              rst_bandmetadata($"bleachingSubdataset", lit(1))
                  .alias("metadata")
            )

        mocks
            .getNetCDFBinaryDf(spark)
            .withColumn("subdatasets", rst_subdatasets($"path"))
            .withColumn("bleachingSubdataset", element_at($"subdatasets", "bleaching_alert_area"))
            .createOrReplaceTempView("source")

        noException should be thrownBy spark.sql("""
                                                   |select rst_bandmetadata(bleachingSubdataset, 1) from source
                                                   |""".stripMargin)

        noException should be thrownBy mocks
            .getNetCDFBinaryDf(spark)
            .withColumn("subdatasets", rst_subdatasets($"path"))
            .withColumn("bleachingSubdataset", element_at($"subdatasets", "bleaching_alert_area"))
            .select(
              rst_bandmetadata($"bleachingSubdataset", lit(1))
                  .alias("metadata")
            )

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
