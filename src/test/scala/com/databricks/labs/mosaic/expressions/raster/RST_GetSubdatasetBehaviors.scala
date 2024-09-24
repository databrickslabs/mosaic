package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.lit
import org.scalatest.matchers.should.Matchers._

trait RST_GetSubdatasetBehaviors extends QueryTest {

    //noinspection MapGetGet
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
            .load("src/test/resources/binary/netcdf-coral")

        val geoReferenceDf = rastersInMemory
            .withColumn("subdataset", rst_getsubdataset($"tile", lit("bleaching_alert_area")))
            .select(rst_georeference($"subdataset"))

        rastersInMemory
            .createOrReplaceTempView("source")

        noException should be thrownBy spark.sql("""
                                                   |select rst_georeference(rst_getsubdataset(tile, "bleaching_alert_area")) from source
                                                   |""".stripMargin)

        val result = geoReferenceDf.as[Map[String, Double]].take(1).head

        result.get("upperLeftX").get != 0.0 shouldBe true
        result.get("upperLeftY").get != 0.0 shouldBe true
        result.get("scaleX").get != 0.0 shouldBe true
        result.get("scaleY").get != 0.0 shouldBe true
        result.get("skewX").get != 0.0 shouldBe false
        result.get("skewY").get != 0.0 shouldBe false

        an[Exception] should be thrownBy spark.sql("""
                                                     |select rst_getsubdataset() from source
                                                     |""".stripMargin)

    }

}
