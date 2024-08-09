package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.{array, lit}
import org.scalatest.matchers.should.Matchers._

trait RST_NDVIBehaviors extends QueryTest {

    // noinspection MapGetGet
    def behaviors(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        val sc = this.spark
        import sc.implicits._
        sc.sparkContext.setLogLevel("ERROR")

        // init
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register(sc)
        import mc.functions._

        val rastersInMemory = spark.read
            .format("gdal")
            .option("pathGlobFilter", "*.TIF")
            .load("src/test/resources/modis")

        val gridTiles = rastersInMemory
            .withColumn("ndvi", rst_ndvi($"tile", lit(1), lit(1)))
            .select("ndvi")

        rastersInMemory
            .createOrReplaceTempView("source")

        noException should be thrownBy spark.sql(
            """
              |select rst_ndvi(tile, 1, 1)
              |  from source
              |""".stripMargin).take(1)

        val result = gridTiles.collect()

        result.length should be(rastersInMemory.count())

    }

}
