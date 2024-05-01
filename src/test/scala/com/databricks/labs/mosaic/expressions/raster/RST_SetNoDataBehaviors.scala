package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.lit
import org.scalatest.matchers.should.Matchers._

trait RST_SetNoDataBehaviors extends QueryTest {

    // noinspection MapGetGet
    def behaviors(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
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
            .withColumn("tile", rst_setnodata($"tile", lit(1)))
            .select("tile")

        rastersInMemory
            .createOrReplaceTempView("source")

        noException should be thrownBy spark.sql(
            """
              |select rst_setnodata(tile, 1)
              |  from source
              |""".stripMargin).take(1)

        noException should be thrownBy spark.sql(
            """
              |select rst_setnodata(tile, array(1.0))
              |  from source
              |""".stripMargin).take(1)

        val result = gridTiles.collect()

        result.length should be(rastersInMemory.count())

    }

}
