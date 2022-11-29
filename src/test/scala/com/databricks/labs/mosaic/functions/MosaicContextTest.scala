package com.databricks.labs.mosaic.functions

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

import scala.util.Try

class MosaicContextTest extends MosaicSpatialQueryTest with SharedSparkSession with MosaicContextBehaviors {

    // Hotfix for SharedSparkSession afterAll cleanup.
    override def afterAll(): Unit = Try(super.afterAll())

    testAllNoCodegen("MosaicContext context creation") { creationOfContext }
    testAllNoCodegen("MosaicContext sql registration") { sqlRegistration }

    test("MosaicContext detect if product H3 is enabled") { productH3Detection() }
    test("MosaicContext lookup correct sql functions") { sqlFunctionLookup() }
    test("MosaicContext should use databricks h3") { callDatabricksH3() }
    test("MosaicContext should correctly reflect functions") { reflectedMethods() }

}
