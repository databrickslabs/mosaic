package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.SparkSuite
import org.scalatest.flatspec.AnyFlatSpec

class TestMosaicExplode extends AnyFlatSpec with MosaicExplodeBehaviors with SparkSuite {

    "Mosaic_Explode" should "decompose wkt geometries for any index system and any geometry API" in {
        it should behave like wktDecompose(MosaicContext.build(H3IndexSystem, ESRI), spark, 8)
        it should behave like wktDecompose(MosaicContext.build(H3IndexSystem, JTS), spark, 8)
        it should behave like wktDecompose(MosaicContext.build(BNGIndexSystem, ESRI), spark, 5)
        it should behave like wktDecompose(MosaicContext.build(BNGIndexSystem, JTS), spark, 5)
    }

    "Mosaic_Explode" should "decompose wkt geometries for any index system and any geometry API with SQL expr" in {
        it should behave like wktDecomposeKeepCoreParamExpression(MosaicContext.build(H3IndexSystem, ESRI), spark, 3)
        it should behave like wktDecomposeKeepCoreParamExpression(MosaicContext.build(H3IndexSystem, JTS), spark, 3)
        it should behave like wktDecomposeKeepCoreParamExpression(MosaicContext.build(BNGIndexSystem, ESRI), spark, 6)
        it should behave like wktDecomposeKeepCoreParamExpression(MosaicContext.build(BNGIndexSystem, JTS), spark, 6)
    }

    "Mosaic_Explode" should "decompose wkt geometries with no null for any index system and any geometry API" in {
        it should behave like wktDecomposeNoNulls(MosaicContext.build(H3IndexSystem, ESRI), spark, 3)
        it should behave like wktDecomposeNoNulls(MosaicContext.build(H3IndexSystem, JTS), spark, 3)
        it should behave like wktDecomposeNoNulls(MosaicContext.build(BNGIndexSystem, ESRI), spark, 6)
        it should behave like wktDecomposeNoNulls(MosaicContext.build(BNGIndexSystem, JTS), spark, 6)
    }

    "Mosaic_Explode" should "decompose wkb geometries for any index system and any geometry API" in {
        it should behave like wkbDecompose(MosaicContext.build(H3IndexSystem, ESRI), spark, 8)
        it should behave like wkbDecompose(MosaicContext.build(H3IndexSystem, JTS), spark, 8)
        it should behave like wkbDecompose(MosaicContext.build(BNGIndexSystem, ESRI), spark, 5)
        it should behave like wkbDecompose(MosaicContext.build(BNGIndexSystem, JTS), spark, 5)
    }

    "Mosaic_Explode" should "decompose hex geometries for any index system and any geometry API" in {
        it should behave like hexDecompose(MosaicContext.build(H3IndexSystem, ESRI), spark, 8)
        it should behave like hexDecompose(MosaicContext.build(H3IndexSystem, JTS), spark, 8)
        it should behave like hexDecompose(MosaicContext.build(BNGIndexSystem, ESRI), spark, 5)
        it should behave like hexDecompose(MosaicContext.build(BNGIndexSystem, JTS), spark, 5)
    }

    "Mosaic_Explode" should "decompose coords geometries for any index system and any geometry API" in {
        it should behave like coordsDecompose(MosaicContext.build(H3IndexSystem, ESRI), spark, 8)
        it should behave like coordsDecompose(MosaicContext.build(H3IndexSystem, JTS), spark, 8)
        it should behave like coordsDecompose(MosaicContext.build(BNGIndexSystem, ESRI), spark, 5)
        it should behave like coordsDecompose(MosaicContext.build(BNGIndexSystem, JTS), spark, 5)
    }

    "Mosaic_Explode" should "decompose lines and multilines for any index system and any geometry API" in {
        it should behave like lineDecompose(MosaicContext.build(H3IndexSystem, ESRI), spark, 3)
        it should behave like lineDecompose(MosaicContext.build(H3IndexSystem, JTS), spark, 3)
        it should behave like lineDecompose(MosaicContext.build(BNGIndexSystem, ESRI), spark, 5)
        it should behave like lineDecompose(MosaicContext.build(BNGIndexSystem, JTS), spark, 5)
    }

}
