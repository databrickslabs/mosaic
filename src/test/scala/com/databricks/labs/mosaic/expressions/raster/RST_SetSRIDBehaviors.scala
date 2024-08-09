package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.raster.gdal.RasterGDAL
import com.databricks.labs.mosaic.functions.{ExprConfig, MosaicContext}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.lit
import org.scalatest.matchers.should.Matchers._

trait RST_SetSRIDBehaviors extends QueryTest {

    def setSRIDBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
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

        val df = rastersInMemory
            .withColumn("result", rst_setsrid($"tile", lit(4326)))
            .select("result")

        // debug
        val sridTile =  df.first.asInstanceOf[GenericRowWithSchema].get(0)
        // info(s"set_srid result -> $sridTile")
        val sridCreateInfo = sridTile.asInstanceOf[GenericRowWithSchema].getAs[Map[String, String]](2)
        // info(s"srid createInfo -> $sridCreateInfo")
        val exprConfigOpt = Option(ExprConfig(sc))
        val sridRaster = RasterGDAL(sridCreateInfo, exprConfigOpt)
        // info(s"get srid -> ${sridRaster.SRID}")

        sridRaster.SRID should be(4326)
        sridRaster.flushAndDestroy() // clean-up

        rastersInMemory
            .createOrReplaceTempView("source")

        noException should be thrownBy spark.sql("""
                                                   |select rst_setsrid(tile, 4326) from source
                                                   |""".stripMargin)

        noException should be thrownBy rastersInMemory
            .withColumn("result", rst_setsrid($"tile", lit(4326)))
            .select("result")

        val result = df
            .where(rst_srid($"result") === lit(4326))
            .collect
            .length

        result > 0 shouldBe true

        an[Exception] should be thrownBy spark.sql("""
                                                     |select rst_setsrid() from source
                                                     |""".stripMargin)

    }

}
