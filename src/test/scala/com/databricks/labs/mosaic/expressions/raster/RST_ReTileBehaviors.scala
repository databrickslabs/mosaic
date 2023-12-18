package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions._
import org.scalatest.matchers.should.Matchers._

trait RST_ReTileBehaviors extends QueryTest {

    def retileBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register()
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val df = mocks
            .getGeotiffBinaryDf(spark)
            .withColumn("result", rst_retile($"path", lit(100), lit(100)))
            .select("result")

        mocks
            .getGeotiffBinaryDf(spark)
            .createOrReplaceTempView("source")

        noException should be thrownBy spark.sql("""
                                                   |select rst_retile(path, 100, 100) from source
                                                   |""".stripMargin)

        noException should be thrownBy mocks
            .getGeotiffBinaryDf(spark)
            .withColumn("result", rst_retile($"path", 100, 100))
            .withColumn("result", rst_retile("/dummy/path", 100, 100))
            .select("result")

        val result = df.as[String].collect().length

        result should be > 0

        an[Exception] should be thrownBy spark.sql("""
                                                     |select rst_retile() from source
                                                     |""".stripMargin)

    }

}
