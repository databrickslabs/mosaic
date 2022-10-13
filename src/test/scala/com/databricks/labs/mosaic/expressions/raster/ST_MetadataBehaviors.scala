package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks
import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.functions.{element_at, lit, map_keys}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

trait ST_MetadataBehaviors extends QueryTest {

    def metadataBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val rasterDfWithMetadata = mocks
            .getGeotiffBinaryDf(spark)
            .select(st_metadata($"content").alias("metadata"))

        val result = rasterDfWithMetadata.as[Map[String, String]].collect()

        result.head.getOrElse("SHORTNAME", "") shouldBe "MCD43A4"
        result.head.getOrElse("ASSOCIATEDINSTRUMENTSHORTNAME", "") shouldBe "MODIS"
        result.head.getOrElse("RANGEBEGINNINGDATE", "") shouldBe "2018-06-26"
        result.head.getOrElse("RANGEENDINGDATE", "") shouldBe "2018-07-11"
        result.head.getOrElse("TileID", "") shouldBe "51010007"

    }

}
