package com.databricks.labs.mosaic.sql

import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.{MosaicSpatialQueryTest, mocks}
import org.scalatest.matchers.must.Matchers.{be, noException}

trait PrettifierBehaviors extends MosaicSpatialQueryTest {

    def behavior(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        mc.register(spark)

        val df = mocks.getWKTRowsDf(mc.getIndexSystem)

        noException should be thrownBy Prettifier.prettified(df, Some(List("wkt")))

    }

}
