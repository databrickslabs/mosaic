package com.databricks.mosaic.expressions.index

import org.scalatest.flatspec.AnyFlatSpec

import com.databricks.mosaic.core.geometry.api.GeometryAPI.{JTS, OGC}
import com.databricks.mosaic.core.index.H3IndexSystem
import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.test.SparkSuite

class TestPolyfill extends AnyFlatSpec with PolyfillBehaviors with SparkSuite {

    "Polyfill" should "fill wkt geometries for any index system and any geometry API" in {
        it should behave like wktPolyfill(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like wktPolyfill(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "Polyfill" should "fill wkb geometries for any index system and any geometry API" in {
        it should behave like wkbPolyfill(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like wkbPolyfill(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "Polyfill" should "fill hex geometries for any index system and any geometry API" in {
        it should behave like hexPolyfill(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like hexPolyfill(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "Polyfill" should "fill coords geometries for any index system and any geometry API" in {
        it should behave like coordsPolyfill(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like coordsPolyfill(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

}
