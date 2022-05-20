package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{JTS, ESRI}
import com.databricks.labs.mosaic.core.index.H3IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.SparkSuite
import org.scalatest.flatspec.AnyFlatSpec

class TestMosaicFill extends AnyFlatSpec with MosaicFillBehaviors with SparkSuite {

    "MosaicFill" should "fill wkt geometries for any index system and any geometry API" in {
        it should behave like wktMosaicFill(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like wktMosaicFill(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "MosaicFill" should "fill wkb geometries for any index system and any geometry API" in {
        it should behave like wkbMosaicFill(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like wkbMosaicFill(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "MosaicFill" should "fill hex geometries for any index system and any geometry API" in {
        it should behave like hexMosaicFill(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like hexMosaicFill(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "MosaicFill" should "fill coords geometries for any index system and any geometry API" in {
        it should behave like coordsMosaicFill(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like coordsMosaicFill(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "MosaicFill" should "fill wkt geometries with keepCoreGeom parameter" in {
        it should behave like wktMosaicFillKeepCoreGeom(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like wktMosaicFillKeepCoreGeom(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

}
