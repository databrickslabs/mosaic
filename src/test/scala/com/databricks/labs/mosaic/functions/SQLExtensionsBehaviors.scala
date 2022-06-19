package com.databricks.labs.mosaic.functions

import com.databricks.labs.mosaic.test.mocks.getHexRowsDf
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.noException
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.SparkSession

trait SQLExtensionsBehaviors { this: AnyFlatSpec =>

    def sqlRegister(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val df = getHexRowsDf(mc).orderBy("id")
        df.createOrReplaceTempView("data")

        noException should be thrownBy spark.sql("""
                                                   | select st_astext(hex) from data limit 1
                                                   |""".stripMargin)
    }

}
