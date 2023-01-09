package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks
import org.apache.spark.sql.QueryTest
import org.scalatest.matchers.should.Matchers._

trait RST_GeoReferenceBehaviors extends QueryTest {

    //noinspection MapGetGet
    def geoReferenceBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register()
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val geoReferenceDf = mocks
            .getNetCDFBinaryDf(spark)
            .withColumn("georeference", rst_georeference($"path"))
            .select("georeference")

        mocks
            .getNetCDFBinaryDf(spark)
            .createOrReplaceTempView("source")

        noException should be thrownBy spark.sql("""
                                                   |select rst_georeference(path) from source
                                                   |""".stripMargin)

        noException should be thrownBy mocks
            .getNetCDFBinaryDf(spark)
            .withColumn("georeference", rst_georeference("/dummy/path"))
            .select("georeference")

        val result = geoReferenceDf.as[Map[String, Double]].collect()

        result.head.get("upperLeftX").get != 0.0 shouldBe false
        result.head.get("upperLeftY").get != 0.0 shouldBe false
        result.head.get("scaleX").get != 0.0 shouldBe true
        result.head.get("scaleY").get != 0.0 shouldBe true
        result.head.get("skewX").get != 0.0 shouldBe false
        result.head.get("skewY").get != 0.0 shouldBe false

        an[Exception] should be thrownBy spark.sql("""
                                                     |select rst_georeference() from source
                                                     |""".stripMargin)

    }

}
