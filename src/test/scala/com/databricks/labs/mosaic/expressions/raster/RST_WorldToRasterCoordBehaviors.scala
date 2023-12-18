package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions._
import org.scalatest.matchers.should.Matchers._

trait RST_WorldToRasterCoordBehaviors extends QueryTest {

    def worldToRasterCoordBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register()
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val df = mocks
            .getNetCDFBinaryDf(spark)
            .withColumn("result", rst_worldtorastercoord($"path", 0, 0))
            .select($"result".getItem("x").as("x"), $"result".getItem("y").as("y"))

        mocks
            .getNetCDFBinaryDf(spark)
            .createOrReplaceTempView("source")

        noException should be thrownBy spark.sql("""
                                                   |select rst_worldtorastercoord(path, 1, 1) from source
                                                   |""".stripMargin)

        noException should be thrownBy mocks
            .getNetCDFBinaryDf(spark)
            .withColumn("result", rst_worldtorastercoord("/dummy/path", 0, 0))
            .withColumn("result", rst_worldtorastercoord($"path", lit(0), lit(0)))
            .select("result")

        noException should be thrownBy df.as[(Int, Int)].collect().head

        an[Exception] should be thrownBy spark.sql("""
                                                     |select rst_worldtorastercoord() from source
                                                     |""".stripMargin)

    }

}
