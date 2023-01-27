package com.databricks.labs.mosaic.functions

import com.databricks.labs.mosaic._
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.expressions.raster.RST_MetaData
import com.databricks.labs.mosaic.functions.MosaicRegistryBehaviors.mosaicContext
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.LongType
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

trait MosaicRegistryBehaviors extends SharedSparkSession {

    def mosaicRegistry(): Unit = {
        val expressionConfig = mosaicContext.expressionConfig
        val registry = spark.sessionState.functionRegistry
        val mosaicRegistry = MosaicRegistry(registry)

        mosaicRegistry.registerExpression[RST_MetaData](expressionConfig)
        mosaicRegistry.registerExpression[RST_MetaData]("rst_metadata_2", expressionConfig)
        mosaicRegistry.registerExpression[RST_MetaData]("rst_metadata_3", RST_MetaData.builder(expressionConfig), expressionConfig)
        mosaicRegistry.registerExpression[RST_MetaData](RST_MetaData.builder(expressionConfig), expressionConfig)

        spark.sessionState.functionRegistry.functionExists(FunctionIdentifier("rst_metadata")) shouldBe true
        spark.sessionState.functionRegistry.functionExists(FunctionIdentifier("rst_metadata_2")) shouldBe true
        spark.sessionState.functionRegistry.functionExists(FunctionIdentifier("rst_metadata_3")) shouldBe true

    }
}

object MosaicRegistryBehaviors extends MockFactory {

    def mosaicContext: MosaicContext = {
        val ix = stub[IndexSystem]
        ix.getCellIdDataType _ when () returns LongType
        ix.name _ when () returns H3.name
        val gapi = stub[GeometryAPI]
        gapi.name _ when () returns JTS.name
        MosaicContext.build(ix, gapi, GDAL)
    }

}
