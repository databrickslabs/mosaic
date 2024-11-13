package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions._
import org.scalatest.matchers.should.Matchers._

trait RST_MedianBehaviors extends QueryTest {

    def largeAreaBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {

        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register()
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val rastersInMemory = spark.read
            .format("gdal")
            .option("raster_storage", "in-memory")
            .load("src/test/resources/modis")

        val df = rastersInMemory
            .withColumn("tile", rst_tessellate($"tile", lit(3)))
            .withColumn("result", rst_median($"tile"))
            .select("result")
            .select(explode($"result").as("result"))

        rastersInMemory
            .withColumn("tile", rst_tessellate($"tile", lit(3)))
            .createOrReplaceTempView("source")

        noException should be thrownBy spark.sql("""
                                                   |select rst_median(tile) from source
                                                   |""".stripMargin)

        noException should be thrownBy rastersInMemory
            .withColumn("result", rst_rastertogridmax($"tile", lit(3)))
            .select("result")

        val result = df.as[Double].collect().max

        result > 0 shouldBe true

        an[Exception] should be thrownBy spark.sql("""
                                                     |select rst_median() from source
                                                     |""".stripMargin)

    }

    def smallAreaBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {

        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register()
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val testRegion = "Polygon ((-8151524 1216659, -8151061 1216659, -8151061 1217123, -8151524 1217123, -8151524 1216659))"

        val rastersInMemory = spark.read
            .format("gdal")
            .option("raster_storage", "in-memory")
            .load("src/test/resources/modis")

        rastersInMemory.createOrReplaceTempView("source")

        val df = rastersInMemory
            .withColumn("tile", rst_clip($"tile", st_buffer(lit(testRegion), lit(-20))))
            .withColumn("result", rst_median($"tile"))
            .select("result")
            .select(explode($"result").as("result"))

        noException should be thrownBy spark.sql("""
                                                   |select rst_median(tile) from source
                                                   |""".stripMargin)

        val result = df.as[Double].collect().max

        result should be > 0.0

    }


}
