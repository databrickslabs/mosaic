package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.lit
import org.scalatest.matchers.should.Matchers._

trait RST_RasterToWorldCoordYBehaviors extends QueryTest {

    def rasterToWorldCoordY(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        val sc = this.spark
        import sc.implicits._
        sc.sparkContext.setLogLevel("ERROR")

        // init
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register(sc)
        import mc.functions._

        val rasterDf = spark.read
            .format("gdal")
            .load("src/test/resources/binary/netcdf-coral")

        val df = rasterDf
            .withColumn("result", rst_rastertoworldcoordy($"tile", lit(2), lit(2)))
            .select("result")

        rasterDf
            .createOrReplaceTempView("source")

        noException should be thrownBy spark.sql("""
                                                   |select rst_rastertoworldcoordy(tile, 2, 2) from source
                                                   |""".stripMargin)

        noException should be thrownBy rasterDf
            .withColumn("result", rst_rastertoworldcoordy(lit("/dummy/path"), 2, 2))
            .withColumn("result", rst_rastertoworldcoordy($"tile", lit(2), lit(2)))
            .select("result")

        val result = df.as[Double].collect().max

        result > 0 shouldBe true

        an[Exception] should be thrownBy spark.sql("""
                                                     |select rst_rastertoworldcoordy() from source
                                                     |""".stripMargin)

        noException should be thrownBy rst_rastertoworldcoordy(lit("/dummy/path"), 2, 2)
        noException should be thrownBy rst_rastertoworldcoordy(lit("/dummy/path"), lit(2), lit(2))

    }

}
