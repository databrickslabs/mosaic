package com.databricks.labs.mosaic.sql

import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.{MosaicSpatialQueryTest, mocks}
import org.scalatest.matchers.must.Matchers.{be, noException}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

trait AnalyzerBehaviors extends MosaicSpatialQueryTest {

    def behavior(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        mc.register(spark)

        val df = mocks.getWKTRowsDf(mc.getIndexSystem)

        val analyzer = MosaicAnalyzer(df)

        mc.getIndexSystem match {
            case H3IndexSystem =>
                noException should be thrownBy analyzer.getOptimalResolution("wkt")
                noException should be thrownBy analyzer.getOptimalResolution("wkt", 0.9)
                noException should be thrownBy analyzer.getOptimalResolution("wkt", 1)

            case BNGIndexSystem =>
                noException should be thrownBy analyzer.getOptimalResolution("wkt")
                noException should be thrownBy analyzer.getOptimalResolutionStr("wkt")

            case _ => noException should be thrownBy analyzer.getOptimalResolution("wkt")
        }

    }

}
