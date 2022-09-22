package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.SparkSuite
import org.scalatest.flatspec.AnyFlatSpec

class TestPolyfill extends AnyFlatSpec with PolyfillBehaviors with SparkSuite {

    "Polyfill" should "fill wkt geometries for any index system and any geometry API" in {
        it should behave like wktPolyfill(MosaicContext.build(H3IndexSystem, ESRI), spark, 11)
        it should behave like wktPolyfill(MosaicContext.build(H3IndexSystem, JTS), spark, 11)
        it should behave like wktPolyfill(MosaicContext.build(BNGIndexSystem, ESRI), spark, 4)
        it should behave like wktPolyfill(MosaicContext.build(BNGIndexSystem, JTS), spark, 4)
    }

    "Polyfill" should "fill wkb geometries for any index system and any geometry API" in {
        it should behave like wkbPolyfill(MosaicContext.build(H3IndexSystem, ESRI), spark, 11)
        it should behave like wkbPolyfill(MosaicContext.build(H3IndexSystem, JTS), spark, 11)
        it should behave like wkbPolyfill(MosaicContext.build(BNGIndexSystem, ESRI), spark, 4)
        it should behave like wkbPolyfill(MosaicContext.build(BNGIndexSystem, JTS), spark, 4)
    }

    "Polyfill" should "fill hex geometries for any index system and any geometry API" in {
        it should behave like hexPolyfill(MosaicContext.build(H3IndexSystem, ESRI), spark, 11)
        it should behave like hexPolyfill(MosaicContext.build(H3IndexSystem, JTS), spark, 11)
        it should behave like hexPolyfill(MosaicContext.build(BNGIndexSystem, ESRI), spark, 4)
        it should behave like hexPolyfill(MosaicContext.build(BNGIndexSystem, JTS), spark, 4)
    }

    "Polyfill" should "fill coords geometries for any index system and any geometry API" in {
        it should behave like coordsPolyfill(MosaicContext.build(H3IndexSystem, ESRI), spark, 11)
        it should behave like coordsPolyfill(MosaicContext.build(H3IndexSystem, JTS), spark, 11)
        it should behave like coordsPolyfill(MosaicContext.build(BNGIndexSystem, ESRI), spark, 4)
        it should behave like coordsPolyfill(MosaicContext.build(BNGIndexSystem, JTS), spark, 4)
    }

    "Polyfill" should "correctly evaluate auxiliary methods." in {
        it should behave like auxiliaryMethods(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like auxiliaryMethods(MosaicContext.build(H3IndexSystem, JTS), spark)
        it should behave like auxiliaryMethods(MosaicContext.build(BNGIndexSystem, ESRI), spark)
        it should behave like auxiliaryMethods(MosaicContext.build(BNGIndexSystem, JTS), spark)
    }

}
