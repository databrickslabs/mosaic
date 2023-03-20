package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.lit
import org.scalatest.matchers.should.Matchers._

trait RST_RasterToWorldCoordXBehaviors extends QueryTest {

    def rasterToWorldCoordX(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register()
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val df = mocks
            .getNetCDFBinaryDf(spark)
            .withColumn("result", rst_rastertoworldcoordx($"path", lit(2), lit(2)))
            .select("result")

        mocks
            .getNetCDFBinaryDf(spark)
            .createOrReplaceTempView("source")

        noException should be thrownBy spark.sql("""
                                                   |select rst_rastertoworldcoordx(path, 2, 2) from source
                                                   |""".stripMargin)

        noException should be thrownBy mocks
            .getNetCDFBinaryDf(spark)
            .withColumn("result", rst_rastertoworldcoordx(lit("/dummy/path"), 2, 2))
            .withColumn("result", rst_rastertoworldcoordx("/dummy/path", lit(2), lit(2)))
            .select("result")

        val result = df.as[Double].collect().max

        result > 0 shouldBe true

        an[Exception] should be thrownBy spark.sql("""
                                                     |select rst_rastertoworldcoordx() from source
                                                     |""".stripMargin)

        noException should be thrownBy rst_rastertoworldcoordx(lit("/dummy/path"), 2, 2)
        noException should be thrownBy rst_rastertoworldcoordx("/dummy/path", lit(2), lit(2))
        noException should be thrownBy rst_rastertoworldcoordx(lit("/dummy/path"), lit(2), lit(2))

    }

}
