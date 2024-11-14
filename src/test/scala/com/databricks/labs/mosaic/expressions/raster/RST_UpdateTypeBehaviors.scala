package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.lit
import org.scalatest.matchers.should.Matchers.{be, convertToAnyShouldWrapper, noException}

trait RST_UpdateTypeBehaviors  extends QueryTest {

    // noinspection MapGetGet
    def behavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register()
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val rastersInMemory = spark.read
            .format("gdal")
            .option("raster_storage", "in-memory")
            .load("src/test/resources/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")

        val newType = "Float32"

        val df = rastersInMemory
            .withColumn("updated_tile", rst_updatetype($"tile", lit(newType)))
            .select(rst_type($"updated_tile").as("new_type"))

        rastersInMemory
            .createOrReplaceTempView("source")

        noException should be thrownBy spark.sql(s"""
                                                   |select rst_updatetype(tile, '$newType') from source
                                                   |""".stripMargin)

        noException should be thrownBy rastersInMemory
            .withColumn("tile", rst_updatetype($"tile", lit(newType)))
            .select("tile")

        val result = df.first.getSeq[String](0).head

        result shouldBe newType

    }

}