package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{JTS, ESRI}
import com.databricks.labs.mosaic.core.index.H3IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.SparkSuite
import org.scalatest.flatspec.AnyFlatSpec

class TestValidity extends AnyFlatSpec with ValidityBehaviors with SparkSuite {

    "ST_IsValid" should "return true for valid geometries for any index system and any geometry API" in {
        it should behave like validGeometries(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like validGeometries(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ST_IsValid" should "return false for invalid geometries for any index system and any geometry API" in {
        it should behave like invalidGeometries(MosaicContext.build(H3IndexSystem, ESRI), spark)
        it should behave like invalidGeometries(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

}
