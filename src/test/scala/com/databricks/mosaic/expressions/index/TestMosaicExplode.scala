package com.databricks.mosaic.expressions.index

import org.scalatest.flatspec.AnyFlatSpec

import com.databricks.mosaic.core.geometry.api.GeometryAPI.{JTS, OGC}
import com.databricks.mosaic.core.index.H3IndexSystem
import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.test.SparkSuite

class TestMosaicExplode extends AnyFlatSpec with MosaicExplodeBehaviors with SparkSuite {

    "Mosaic_Explode" should "decompose wkt geometries for any index system and any geometry API" in {
        it should behave like wktDecompose(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like wktDecompose(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "Mosaic_Explode" should "decompose wkb geometries for any index system and any geometry API" in {
        it should behave like wkbDecompose(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like wkbDecompose(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "Mosaic_Explode" should "decompose hex geometries for any index system and any geometry API" in {
        it should behave like hexDecompose(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like hexDecompose(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "Mosaic_Explode" should "decompose coords geometries for any index system and any geometry API" in {
        it should behave like coordsDecompose(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like coordsDecompose(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

}
