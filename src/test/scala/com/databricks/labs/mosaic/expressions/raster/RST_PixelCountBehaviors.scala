package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions._
import org.scalatest.matchers.should.Matchers._

trait RST_PixelCountBehaviors extends QueryTest {

    def behavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
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
        //info(s"row -> ${rastersInMemory.first().toSeq.toString()}")

        val dfPix = rastersInMemory
            .select(rst_pixelcount($"tile"))
        info(s"pixelcount (prior to tessellate) -> ${dfPix.first().toSeq.toString()}")

        // this should work after rst_tessellate

        val df = rastersInMemory
            .withColumn("tile", rst_tessellate($"tile", lit(3)))
            .withColumn("result", rst_pixelcount($"tile"))
            .select("result")
            .select(explode($"result").as("result"))

        //info(df.first().toSeq.toString())


        rastersInMemory
            .withColumn("tile", rst_tessellate($"tile", lit(3)))
            .createOrReplaceTempView("source")

        // TODO: modified to 3 args... should this be revisited?
        noException should be thrownBy spark.sql("""
                                                   |select rst_pixelcount(tile,false,false) from source
                                                   |""".stripMargin)

        noException should be thrownBy rastersInMemory
            .withColumn("result", rst_rastertogridmax($"tile", lit(3)))
            .select("result")

        val result = df.as[Double].collect().max

        result > 0 shouldBe true

        an[Exception] should be thrownBy spark.sql("""
                                                     |select rst_pixelcount() from source
                                                     |""".stripMargin)

    }

}
