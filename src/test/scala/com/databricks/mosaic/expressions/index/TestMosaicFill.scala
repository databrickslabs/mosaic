package com.databricks.mosaic.expressions.index

import org.scalatest.flatspec.AnyFlatSpec

import com.databricks.mosaic.core.geometry.api.GeometryAPI.{JTS, OGC}
import com.databricks.mosaic.core.index.H3IndexSystem
import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.test.SparkSuite

class TestMosaicFill extends AnyFlatSpec with MosaicFillBehaviors with SparkSuite {

    "MosaicFill" should "fill wkt geometries for any index system and any geometry API" in {
        it should behave like wktMosaicFill(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like wktMosaicFill(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "MosaicFill" should "fill wkb geometries for any index system and any geometry API" in {
        it should behave like wkbMosaicFill(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like wkbMosaicFill(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "MosaicFill" should "fill hex geometries for any index system and any geometry API" in {
        it should behave like hexMosaicFill(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like hexMosaicFill(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "MosaicFill" should "fill coords geometries for any index system and any geometry API" in {
        it should behave like coordsMosaicFill(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like coordsMosaicFill(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

}
