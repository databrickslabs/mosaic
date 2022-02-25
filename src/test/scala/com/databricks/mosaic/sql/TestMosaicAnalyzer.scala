//package com.databricks.mosaic.sql
//
//import org.scalatest.flatspec.AnyFlatSpec
//
//import com.databricks.mosaic.core.geometry.api.GeometryAPI.{JTS, OGC}
//import com.databricks.mosaic.core.index.H3IndexSystem
//import com.databricks.mosaic.functions.MosaicContext
//import com.databricks.mosaic.test.SparkSuite
//
//class TestMosaicAnalyzer extends AnyFlatSpec with MosaicAnalyzerBehaviors with SparkSuite {
//  "MosaicAnalyzer" should "return the optimal resolution for any index system and any geometry API" in {
//    it should behave like optimalResolution(MosaicContext.build(H3IndexSystem, OGC), spark)
//    it should behave like optimalResolution(MosaicContext.build(H3IndexSystem, JTS), spark)
//  }
//}
