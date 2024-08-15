package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions._
import org.scalatest.matchers.should.Matchers._

trait RST_MedianBehaviors extends QueryTest {

    def behavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        val sc = this.spark
        import sc.implicits._
        sc.sparkContext.setLogLevel("ERROR")

        // init
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register(sc)
        import mc.functions._

        val rasterDf = spark.read
            .format("gdal")
            .option("pathGlobFilter", "*.TIF")
            .load("src/test/resources/modis")

        val df = rasterDf
            .withColumn("tile", rst_tessellate($"tile", lit(3)))
            .withColumn("result", rst_median($"tile"))
            .select("result")
            .select(explode($"result").as("result"))

        rasterDf
            .withColumn("tile", rst_tessellate($"tile", lit(3)))
            .createOrReplaceTempView("source")

        noException should be thrownBy spark.sql("""
                                                   |select rst_median(tile) from source
                                                   |""".stripMargin)

        noException should be thrownBy rasterDf
            .withColumn("result", rst_rastertogridmax($"tile", lit(3)))
            .select("result")

        val result = df.as[Double].collect().max

        result > 0 shouldBe true

        an[Exception] should be thrownBy spark.sql("""
                                                     |select rst_median() from source
                                                     |""".stripMargin)

    }

}
