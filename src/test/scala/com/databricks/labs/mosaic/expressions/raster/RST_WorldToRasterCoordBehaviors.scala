package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions._
import org.scalatest.matchers.should.Matchers._

trait RST_WorldToRasterCoordBehaviors extends QueryTest {

    def worldToRasterCoordBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
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
            .withColumn("result", rst_worldtorastercoord($"tile", 0, 0))
            .select($"result".getItem("x").as("x"), $"result".getItem("y").as("y"))

        rastersInMemory
            .createOrReplaceTempView("source")

        noException should be thrownBy spark.sql("""
                                                   |select rst_worldtorastercoord(tile, 1, 1) from source
                                                   |""".stripMargin)

        noException should be thrownBy rastersInMemory
            .withColumn("result", rst_worldtorastercoord($"tile", 0, 0))
            .withColumn("result", rst_worldtorastercoord($"tile", lit(0), lit(0)))
            .select("result")

        noException should be thrownBy df.as[(Int, Int)].collect().head

        an[Exception] should be thrownBy spark.sql("""
                                                     |select rst_worldtorastercoord() from source
                                                     |""".stripMargin)

    }

}
