package com.databricks.labs.mosaic.models

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.SparkSuite
import org.scalatest.flatspec.AnyFlatSpec

class TestApproximateSpatialKNN extends AnyFlatSpec with ApproximateSpatialKNNBehaviors with SparkSuite {

    "Polyfill" should "fill wkt geometries for any index system and any geometry API" in {
        it should behave like wktKNN(MosaicContext.build(H3IndexSystem, ESRI), spark, 11)
//        it should behave like wktKNN(MosaicContext.build(H3IndexSystem, JTS), spark, 11)
//        it should behave like wktKNN(MosaicContext.build(BNGIndexSystem, ESRI), spark, 4)
//        it should behave like wktKNN(MosaicContext.build(BNGIndexSystem, JTS), spark, 4)
    }

}
