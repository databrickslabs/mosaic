package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.scalatest.matchers.should.Matchers._

trait RST_GetNoDataBehaviors extends QueryTest {

    //noinspection MapGetGet
    def behaviors(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register()
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val rastersInMemory = spark.read
            .format("gdal")
            .option("raster_storage", "in-memory")
            .load("src/test/resources/modis/")

        val noDataVals = rastersInMemory
            .withColumn("no_data", rst_getnodata($"tile"))
            .select("no_data")

        rastersInMemory
            .createOrReplaceTempView("source")

        noException should be thrownBy spark.sql("""
                                                   |select rst_getnodata(tile) from source
                                                   |""".stripMargin)

        noException should be thrownBy rastersInMemory
            .withColumn("no_data", rst_getnodata($"tile"))
            .select("no_data")

        val result = noDataVals.as[Seq[Double]].collect()

        result.forall(_.forall(_ == 32767.0)) should be(true)

        an[Exception] should be thrownBy spark.sql("""
                                                     |select rst_getnodata() from source
                                                     |""".stripMargin)

    }

}