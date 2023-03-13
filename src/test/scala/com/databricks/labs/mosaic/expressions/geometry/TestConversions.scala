package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.SparkSuite
import org.scalatest.flatspec.AnyFlatSpec

class TestConversions extends AnyFlatSpec with ConversionBehaviors with SparkSuite {

    "conversion_expressions" should "convert correctly" in {
        it should behave like conversion_expressions(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like conversion_expressions(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "conversion_functions" should "convert correctly" in {
        it should behave like conversion_functions(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like conversion_functions(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

}
