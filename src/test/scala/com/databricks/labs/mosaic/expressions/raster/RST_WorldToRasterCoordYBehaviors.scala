package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions._
import org.scalatest.matchers.should.Matchers._

trait RST_WorldToRasterCoordYBehaviors extends QueryTest {

    def worldToRasterCoordYBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register()
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val df = mocks
            .getNetCDFBinaryDf(spark)
            .withColumn("result", rst_worldtorastercoordy($"path", 0, 0))
            .select("result")

        mocks
            .getNetCDFBinaryDf(spark)
            .createOrReplaceTempView("source")

        noException should be thrownBy spark.sql("""
                                                   |select rst_worldtorastercoordy(path, 1, 1) from source
                                                   |""".stripMargin)

        noException should be thrownBy mocks
            .getNetCDFBinaryDf(spark)
            .withColumn("result", rst_worldtorastercoordy("/dummy/path", 0, 0))
            .withColumn("result", rst_worldtorastercoordy($"path", lit(0), lit(0)))
            .select("result")

        val result = df.as[Double].collect().head

        result == 0 shouldBe true

        an[Exception] should be thrownBy spark.sql("""
                                                     |select rst_worldtorastercoordy() from source
                                                     |""".stripMargin)

    }

}
