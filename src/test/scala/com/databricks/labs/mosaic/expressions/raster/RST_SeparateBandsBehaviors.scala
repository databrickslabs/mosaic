package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.{BAND_META_GET_KEY, RASTER_PATH_KEY}
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.raster.io.RasterIO
import com.databricks.labs.mosaic.functions.{ExprConfig, MosaicContext}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.scalatest.matchers.should.Matchers._

trait RST_SeparateBandsBehaviors extends QueryTest {

    def separateBandsBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        val sc = this.spark
        import sc.implicits._
        sc.sparkContext.setLogLevel("ERROR")

        // init
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register(sc)
        import mc.functions._

        val rastersInMemory = spark.read
            .format("gdal")
            .load("src/test/resources/binary/netcdf-CMIP5/prAdjust_day_HadGEM2-CC_SMHI-DBSrev930-GFD-1981-2010-postproc_rcp45_r1i1p1_20201201-20201231.nc")

        val df = rastersInMemory
            .withColumn("result", rst_separatebands($"tile"))
            .select("result")

        val r = df.first().asInstanceOf[GenericRowWithSchema].get(0)
        val createInfo = r.asInstanceOf[GenericRowWithSchema].getAs[Map[String, String]](2)
        val path = createInfo(RASTER_PATH_KEY)
        val dsOpt = RasterIO.rawPathAsDatasetOpt(path, driverNameOpt = None, Some(ExprConfig(sc)))
        info(s"separate bands result -> $createInfo")
        //info(s"ds metadata -> ${dsOpt.get.GetMetadata_Dict()}")
        val metaKey = s"NC_GLOBAL#$BAND_META_GET_KEY"
        info(s"band idx (from metadata)? ${dsOpt.get.GetMetadataItem(metaKey)}")

        rastersInMemory
            .createOrReplaceTempView("source")

        noException should be thrownBy spark.sql("""
                                                   |select rst_separatebands(tile) from source
                                                   |""".stripMargin)

        noException should be thrownBy rastersInMemory
            .withColumn("result", rst_separatebands($"tile"))
            .withColumn("result", rst_separatebands($"tile"))
            .select("result")

        val result = df.collect().length

        result should be > 0

        an[Exception] should be thrownBy spark.sql("""
                                                     |select rst_separatebands() from source
                                                     |""".stripMargin)

    }

}
