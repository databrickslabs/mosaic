package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks
import org.apache.spark.sql.QueryTest
import org.scalatest.matchers.should.Matchers._

trait RST_MetadataBehaviors extends QueryTest {

    def metadataBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register()
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val rasterDfWithMetadata = mocks
            .getGeotiffBinaryDf(spark)
            .select(
              rst_metadata($"path").alias("metadata")
            )
            .select("metadata")

        val result = rasterDfWithMetadata.as[Map[String, String]].collect()

        mocks
            .getGeotiffBinaryDf(spark)
            .createOrReplaceTempView("source")

        noException should be thrownBy spark.sql("""
                                                   |select rst_metadata(path) from source
                                                   |""".stripMargin)

        result.head.getOrElse("SHORTNAME", "") shouldBe "MCD43A4"
        result.head.getOrElse("ASSOCIATEDINSTRUMENTSHORTNAME", "") shouldBe "MODIS"
        result.head.getOrElse("RANGEBEGINNINGDATE", "") shouldBe "2018-06-26"
        result.head.getOrElse("RANGEENDINGDATE", "") shouldBe "2018-07-11"
        result.head.getOrElse("TileID", "") shouldBe "51010007"

        noException should be thrownBy rst_metadata($"path")
        noException should be thrownBy rst_metadata("path")

    }

}
