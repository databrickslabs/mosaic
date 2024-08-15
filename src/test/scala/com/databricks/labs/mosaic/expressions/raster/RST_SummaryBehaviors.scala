package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.scalatest.matchers.should.Matchers._

trait RST_SummaryBehaviors extends QueryTest {

    def summaryBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        val sc = this.spark
        import sc.implicits._
        sc.sparkContext.setLogLevel("ERROR")

        // init
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register(sc)
        import mc.functions._

        val rasterDf = spark.read
            .format("gdal")
            .load("src/test/resources/binary/netcdf-coral")

        val df = rasterDf
            .withColumn("result", rst_summary($"tile"))
            .select("result")

        rasterDf
            .createOrReplaceTempView("source")

        noException should be thrownBy spark.sql("""
                                                   |select rst_summary(tile) from source
                                                   |""".stripMargin)

        noException should be thrownBy rasterDf
            .withColumn("result", rst_summary($"tile"))
            .select("result")

        val result = df.as[String].collect().head.length

        result should be > 0

        an[Exception] should be thrownBy spark.sql("""
                                                     |select rst_summary() from source
                                                     |""".stripMargin)

    }

}
