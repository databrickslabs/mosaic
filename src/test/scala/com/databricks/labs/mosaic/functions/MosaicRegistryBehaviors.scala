package com.databricks.labs.mosaic.functions

import com.databricks.labs.mosaic.expressions.raster.ST_MetaData
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

trait MosaicRegistryBehaviors extends SharedSparkSession {

    def mosaicRegistry(): Unit = {
        val registry = spark.sessionState.functionRegistry
        val mosaicRegistry = MosaicRegistry(registry)

        mosaicRegistry.registerExpression[ST_MetaData]()
        mosaicRegistry.registerExpression[ST_MetaData]("st_metadata_2")
        mosaicRegistry.registerExpression[ST_MetaData]("st_metadata_3", ST_MetaData.builder)
        mosaicRegistry.registerExpression[ST_MetaData](ST_MetaData.builder)

        spark.sessionState.functionRegistry.functionExists(FunctionIdentifier("st_metadata")) shouldBe true
        spark.sessionState.functionRegistry.functionExists(FunctionIdentifier("st_metadata_2")) shouldBe true
        spark.sessionState.functionRegistry.functionExists(FunctionIdentifier("st_metadata_3")) shouldBe true

    }
}
