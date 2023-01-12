package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks
import org.apache.spark.sql.QueryTest
import org.scalatest.matchers.should.Matchers._

trait RST_IsEmptyBehaviors extends QueryTest {

    // noinspection AccessorLikeMethodIsUnit
    def isEmptyBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register()
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val df = mocks
            .getNetCDFBinaryDf(spark)
            .withColumn("result", rst_isempty($"path"))
            .select("result")

        mocks
            .getNetCDFBinaryDf(spark)
            .createOrReplaceTempView("source")

        noException should be thrownBy spark.sql("""
                                                   |select rst_isempty(path) from source
                                                   |""".stripMargin)

        noException should be thrownBy mocks
            .getNetCDFBinaryDf(spark)
            .withColumn("result", rst_isempty("/dummy/path"))
            .select("result")

        val result = df.as[Boolean].collect()

        result.head shouldBe false

        an[Exception] should be thrownBy spark.sql("""
                                                     |select rst_isempty(path, 1, 1) from source
                                                     |""".stripMargin)

    }

}
