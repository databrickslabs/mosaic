package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions._

import org.scalatest.matchers.should.Matchers._

trait RST_ReTileBehaviors extends QueryTest {

    def retileBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
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
            .withColumn("result", rst_retile($"tile", lit(400), lit(400)))
            .select("result")

        rasterDf
            .createOrReplaceTempView("source")

        noException should be thrownBy spark.sql("""
                                                   |select rst_retile(tile, 400, 400) from source
                                                   |""".stripMargin)

        noException should be thrownBy rasterDf
            .withColumn("result", rst_retile($"tile", 400, 400))
            .withColumn("result", rst_retile($"tile", 400, 400))
            .select("result")

        val result = df.collect().length

        result should be > 0

        an[Exception] should be thrownBy spark.sql("""
                                                     |select rst_retile() from source
                                                     |""".stripMargin)

    }

}
