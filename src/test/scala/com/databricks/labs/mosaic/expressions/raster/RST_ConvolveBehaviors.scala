package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.{array, lit}
import org.scalatest.matchers.should.Matchers._

trait RST_ConvolveBehaviors extends QueryTest {

    // noinspection MapGetGet
    def behaviors(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register()
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val rastersInMemory = spark.read
            .format("gdal")
            .option("raster_storage", "in-memory")
            .load("src/test/resources/modis")

        val gridTiles = rastersInMemory
            .withColumn("result", rst_convolve($"tile", array(array(lit(1.0), lit(2.0), lit(3.0)), array(lit(3.0), lit(2.0), lit(1.0)), array(lit(1.0), lit(3.0), lit(2.0)))))
            .select("result")
            .collect()

        gridTiles.length should be(7)

        rastersInMemory.createOrReplaceTempView("source")

       spark
            .sql("""
                   |select rst_convolve(tile, array(array(1, 2, 3), array(2, 3, 1), array(1, 1, 1))) as tile from source
                   |""".stripMargin)
            .collect()

    }

}
