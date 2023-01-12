package com.databricks.labs.mosaic.sql.extensions

import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks.getHexRowsDf
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.{be, noException}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

trait SQLExtensionsBehaviors { this: AnyFlatSpec =>

    def sqlRegister(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val df = getHexRowsDf(mc).orderBy("id")
        df.createOrReplaceTempView("data")

        noException should be thrownBy spark.sql("""
                                                   | select st_astext(hex) from data limit 1
                                                   |""".stripMargin)
    }

    def mosaicGDAL(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        import com.databricks.labs.mosaic.gdal.MosaicGDAL

        spark.sql("show functions") // triggers the gdal enable inject rule
        MosaicGDAL.isEnabled shouldBe true
    }

}
