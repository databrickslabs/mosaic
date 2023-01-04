package com.databricks.labs.mosaic.functions

import com.databricks.labs.mosaic.expressions.raster.RST_MetaData
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

trait MosaicRegistryBehaviors extends SharedSparkSession {

    def mosaicRegistry(): Unit = {
        val registry = spark.sessionState.functionRegistry
        val mosaicRegistry = MosaicRegistry(registry)

        mosaicRegistry.registerExpression[RST_MetaData]()
        mosaicRegistry.registerExpression[RST_MetaData]("rst_metadata_2")
        mosaicRegistry.registerExpression[RST_MetaData]("rst_metadata_3", RST_MetaData.builder("GDAL"))
        mosaicRegistry.registerExpression[RST_MetaData](RST_MetaData.builder("GDAL"))

        spark.sessionState.functionRegistry.functionExists(FunctionIdentifier("rst_metadata")) shouldBe true
        spark.sessionState.functionRegistry.functionExists(FunctionIdentifier("rst_metadata_2")) shouldBe true
        spark.sessionState.functionRegistry.functionExists(FunctionIdentifier("rst_metadata_3")) shouldBe true

    }
}
