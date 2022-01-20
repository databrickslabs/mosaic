package com.databricks.mosaic.codegen

import org.scalatest.flatspec.AnyFlatSpec

import com.databricks.mosaic.core.geometry.api.GeometryAPI.{JTS, OGC}
import com.databricks.mosaic.core.index.H3IndexSystem
import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.test.SparkSuite

class TestAsHexCodegen extends AnyFlatSpec with AsHexCodegenBehaviors with SparkSuite {

    "AsHex Expression" should "do codegen for any index system and any geometry API" in {
        spark.conf.set("spark.sql.codegen.factoryMode", "CODEGEN_ONLY")
        spark.conf.set("spark.sql.codegen.fallback", "false")

        it should behave like codeGeneration(MosaicContext.build(H3IndexSystem, OGC))
        it should behave like codeGeneration(MosaicContext.build(H3IndexSystem, JTS))
    }

}
