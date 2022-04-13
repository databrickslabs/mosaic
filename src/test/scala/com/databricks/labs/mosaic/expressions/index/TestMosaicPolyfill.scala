package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{JTS, ESRI}
import com.databricks.labs.mosaic.core.index.H3IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.SparkSuite
import org.scalatest.flatspec.AnyFlatSpec

class TestMosaicPolyfill extends AnyFlatSpec with PolyfillBehaviors with SparkSuite {

    "Polyfill" should "fill wkt geometries for any index system and any geometry API" in {
        it should behave like wktPolyfill(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like wktPolyfill(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "Polyfill" should "fill wkb geometries for any index system and any geometry API" in {
        it should behave like wkbPolyfill(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like wkbPolyfill(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "Polyfill" should "fill hex geometries for any index system and any geometry API" in {
        it should behave like hexPolyfill(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like hexPolyfill(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "Polyfill" should "fill coords geometries for any index system and any geometry API" in {
        it should behave like coordsPolyfill(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like coordsPolyfill(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

}
