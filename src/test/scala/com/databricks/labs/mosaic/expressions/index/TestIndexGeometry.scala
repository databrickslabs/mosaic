package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.SparkSuite
import org.scalatest.flatspec.AnyFlatSpec

class TestIndexGeometry extends AnyFlatSpec with IndexGeometryBehaviors with SparkSuite {

    "IndexGeometry" should "correctly evaluate auxiliary methods." in {
        it should behave like auxiliaryMethods(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like auxiliaryMethods(MosaicContext.build(H3IndexSystem, JTS), spark)
        it should behave like auxiliaryMethods(MosaicContext.build(BNGIndexSystem, ESRI), spark)
        it should behave like auxiliaryMethods(MosaicContext.build(BNGIndexSystem, JTS), spark)
    }

}
