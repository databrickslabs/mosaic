package com.databricks.mosaic.expressions.geometry

import org.scalatest.flatspec.AnyFlatSpec

import com.databricks.mosaic.core.geometry.api.GeometryAPI.{JTS, OGC}
import com.databricks.mosaic.core.index.H3IndexSystem
import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.test.SparkCodeGenSuite

class TestCoordinateMinMaxCodegen extends AnyFlatSpec with CoordinateMinMaxBehaviors with SparkCodeGenSuite {

    "ST_xmin" should "return minimum x coordinate for any index system and any geometry API" in {
        it should behave like xMinCodegen(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like xMinCodegen(MosaicContext.build(H3IndexSystem, JTS), spark)
        it should behave like xMin(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like xMin(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ST_xmax" should "return maximum x coordinate for any index system and any geometry API" in {
        it should behave like xMaxCodegen(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like xMaxCodegen(MosaicContext.build(H3IndexSystem, JTS), spark)
        it should behave like xMax(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like xMax(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ST_ymin" should "return minimum y coordinate for any index system and any geometry API" in {
        it should behave like yMinCodegen(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like yMinCodegen(MosaicContext.build(H3IndexSystem, JTS), spark)
        it should behave like yMin(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like yMin(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ST_ymax" should "return maximum y coordinate for any index system and any geometry API" in {
        it should behave like yMaxCodegen(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like yMaxCodegen(MosaicContext.build(H3IndexSystem, JTS), spark)
        it should behave like yMax(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like yMax(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

}
