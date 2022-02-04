package com.databricks.mosaic.expressions.geometry

import org.scalatest.flatspec.AnyFlatSpec

import com.databricks.mosaic.core.geometry.api.GeometryAPI.{JTS, OGC}
import com.databricks.mosaic.core.index.H3IndexSystem
import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.test.SparkCodeGenSuite

class TestTypeCheckCodegen extends AnyFlatSpec with TypeCheckBehaviors with SparkCodeGenSuite {


    "ST_GeometryType" should "return correct types for wkt format for any index system and any geometry API" in {
        it should behave like wktTypesCodegen(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like wktTypesCodegen(MosaicContext.build(H3IndexSystem, JTS), spark)
        it should behave like wktTypes(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like wktTypes(MosaicContext.build(H3IndexSystem, JTS), spark)
    }

    "ST_GeometryType" should "return correct types for hex format for any index system and any geometry API" in {
        it should behave like hexTypesCodegen(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like hexTypesCodegen(MosaicContext.build(H3IndexSystem, JTS), spark)
        it should behave like hexTypes(MosaicContext.build(H3IndexSystem, OGC), spark)
        it should behave like hexTypes(MosaicContext.build(H3IndexSystem, JTS), spark)
    }


}
