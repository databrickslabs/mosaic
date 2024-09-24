package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.scalatest.matchers.should.Matchers._

trait RST_IsEmptyBehaviors extends QueryTest {

    // noinspection AccessorLikeMethodIsUnit
    def isEmptyBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register()
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val rastersInMemory = spark.read
            .format("gdal")
            .option("raster_storage", "in-memory")
            .load("src/test/resources/binary/netcdf-coral")

        val df = rastersInMemory
            .withColumn("result", rst_isempty($"tile"))
            .select("result")

        val df2 = rastersInMemory
            .withColumn("tile", rst_getsubdataset($"tile", "bleaching_alert_area"))
            .withColumn("result", rst_isempty($"tile"))
            .select("result")

        rastersInMemory
            .createOrReplaceTempView("source")

        noException should be thrownBy spark.sql("""
                                                   |select rst_isempty(tile) from source
                                                   |""".stripMargin)

        noException should be thrownBy rastersInMemory
            .withColumn("result", rst_isempty($"tile"))
            .select("result")

        val result = df.as[Boolean].collect()
        val result2 = df2.as[Boolean].collect()

        result.head shouldBe false
        result2.head shouldBe false

        an[Exception] should be thrownBy spark.sql("""
                                                     |select rst_isempty() from source
                                                     |""".stripMargin)

    }

}
