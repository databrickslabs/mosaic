package com.databricks.labs.mosaic.sql.extensions

import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks.getHexRowsDf
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.{be, noException}

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
