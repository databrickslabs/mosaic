package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks
import org.apache.spark.sql.QueryTest
import org.scalatest.matchers.should.Matchers._

trait ST_SubdatasetsBehaviors extends QueryTest {

    def subdatasetsBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register()
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val rasterDfWithSubdatasets = mocks
            .getNetCDFBinaryDf(spark)
            .select(
              st_subdatasets($"content")
                  .alias("subdatasets")
            )

        val result = rasterDfWithSubdatasets.as[Map[String, String]].collect()

        mocks
            .getGeotiffBinaryDf(spark)
            .createOrReplaceTempView("source")

        noException should be thrownBy spark.sql("""
                                                   |select st_subdatasets(content) from source
                                                   |""".stripMargin)

        noException should be thrownBy spark.sql("""
                                                   |select st_subdatasets(content, "") from source
                                                   |""".stripMargin)

        an[Exception] should be thrownBy spark.sql("""
                                                     |select st_subdatasets(content, "", 1) from source
                                                     |""".stripMargin)

        result.head.keys.toList.length shouldBe 2
        result.head.values.toList should contain allElementsOf List(
          "[1x3600x7200] //bleaching_alert_area (8-bit unsigned character)",
          "[1x3600x7200] //mask (8-bit unsigned character)"
        )

    }

}
