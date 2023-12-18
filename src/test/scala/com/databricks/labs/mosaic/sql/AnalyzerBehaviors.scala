package com.databricks.labs.mosaic.sql

import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.{MosaicSpatialQueryTest, mocks}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

trait AnalyzerBehaviors extends MosaicSpatialQueryTest {

    def behavior(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = mosaicContext
        mc.register(spark)

        val df = mocks.getWKTRowsDf(mc.getIndexSystem)

        val analyzer = MosaicAnalyzer(df)

        mc.getIndexSystem match {
            case H3IndexSystem =>
                analyzer.getOptimalResolution("wkt") shouldEqual 1
                analyzer.getOptimalResolution("wkt", 0.9) shouldEqual 1
                analyzer.getOptimalResolution("wkt", 1) shouldEqual 1

            case BNGIndexSystem =>
                analyzer.getOptimalResolution("wkt") shouldEqual -3
                analyzer.getOptimalResolutionStr("wkt") shouldEqual "5km"

            case _ => analyzer.getOptimalResolution("wkt") shouldEqual 6
        }

    }

}
