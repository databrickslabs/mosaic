package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ST_MinMaxXYZTest extends QueryTest with SharedSparkSession with ST_MinMaxXYZBehaviors {

    private val noCodegen =
        withSQLConf(
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
          SQLConf.CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.NO_CODEGEN.toString
        ) _

    private val codegenOnly =
        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
          SQLConf.CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.CODEGEN_ONLY.toString
        ) _

    test("Testing xMin (H3, JTS) NO_CODEGEN") { noCodegen { xMinBehavior(H3IndexSystem, JTS) } }
    test("Testing xMin (H3, ESRI) NO_CODEGEN") { noCodegen { xMinBehavior(H3IndexSystem, ESRI) } }
    test("Testing xMin (BNG, JTS) NO_CODEGEN") { noCodegen { xMinBehavior(BNGIndexSystem, JTS) } }
    test("Testing xMin (BNG, ESRI) NO_CODEGEN") { noCodegen { xMinBehavior(BNGIndexSystem, ESRI) } }
    test("Testing xMin (H3, JTS) CODEGEN compilation") { codegenOnly { xMinCodegen(H3IndexSystem, JTS) } }
    test("Testing xMin (H3, ESRI) CODEGEN compilation") { codegenOnly { xMinCodegen(H3IndexSystem, ESRI) } }
    test("Testing xMin (BNG, JTS) CODEGEN compilation") { codegenOnly { xMinCodegen(BNGIndexSystem, JTS) } }
    test("Testing xMin (BNG, ESRI) CODEGEN compilation") { codegenOnly { xMinCodegen(BNGIndexSystem, ESRI) } }
    test("Testing xMin (H3, JTS) CODEGEN_ONLY") { codegenOnly { xMinBehavior(H3IndexSystem, JTS) } }
    test("Testing xMin (H3, ESRI) CODEGEN_ONLY") { codegenOnly { xMinBehavior(H3IndexSystem, ESRI) } }
    test("Testing xMin (BNG, JTS) CODEGEN_ONLY") { codegenOnly { xMinBehavior(BNGIndexSystem, JTS) } }
    test("Testing xMin (BNG, ESRI) CODEGEN_ONLY") { codegenOnly { xMinBehavior(BNGIndexSystem, ESRI) } }

    test("Testing xMax (H3, JTS) NO_CODEGEN") { noCodegen { xMaxBehavior(H3IndexSystem, JTS) } }
    test("Testing xMax (H3, ESRI) NO_CODEGEN") { noCodegen { xMaxBehavior(H3IndexSystem, ESRI) } }
    test("Testing xMax (BNG, JTS) NO_CODEGEN") { noCodegen { xMaxBehavior(BNGIndexSystem, JTS) } }
    test("Testing xMax (BNG, ESRI) NO_CODEGEN") { noCodegen { xMaxBehavior(BNGIndexSystem, ESRI) } }
    test("Testing xMax (H3, JTS) CODEGEN compilation") { codegenOnly { xMaxCodegen(H3IndexSystem, JTS) } }
    test("Testing xMax (H3, ESRI) CODEGEN compilation") { codegenOnly { xMaxCodegen(H3IndexSystem, ESRI) } }
    test("Testing xMax (BNG, JTS) CODEGEN compilation") { codegenOnly { xMaxCodegen(BNGIndexSystem, JTS) } }
    test("Testing xMax (BNG, ESRI) CODEGEN compilation") { codegenOnly { xMaxCodegen(BNGIndexSystem, ESRI) } }
    test("Testing xMax (H3, JTS) CODEGEN_ONLY") { codegenOnly { xMaxBehavior(H3IndexSystem, JTS) } }
    test("Testing xMax (H3, ESRI) CODEGEN_ONLY") { codegenOnly { xMaxBehavior(H3IndexSystem, ESRI) } }
    test("Testing xMax (BNG, JTS) CODEGEN_ONLY") { codegenOnly { xMaxBehavior(BNGIndexSystem, JTS) } }
    test("Testing xMax (BNG, ESRI) CODEGEN_ONLY") { codegenOnly { xMaxBehavior(BNGIndexSystem, ESRI) } }

    test("Testing yMin (H3, JTS) NO_CODEGEN") { noCodegen { yMinBehavior(H3IndexSystem, JTS) } }
    test("Testing yMin (H3, ESRI) NO_CODEGEN") { noCodegen { yMinBehavior(H3IndexSystem, ESRI) } }
    test("Testing yMin (BNG, JTS) NO_CODEGEN") { noCodegen { yMinBehavior(BNGIndexSystem, JTS) } }
    test("Testing yMin (BNG, ESRI) NO_CODEGEN") { noCodegen { yMinBehavior(BNGIndexSystem, ESRI) } }
    test("Testing yMin (H3, JTS) CODEGEN compilation") { codegenOnly { yMinCodegen(H3IndexSystem, JTS) } }
    test("Testing yMin (H3, ESRI) CODEGEN compilation") { codegenOnly { yMinCodegen(H3IndexSystem, ESRI) } }
    test("Testing yMin (BNG, JTS) CODEGEN compilation") { codegenOnly { yMinCodegen(BNGIndexSystem, JTS) } }
    test("Testing yMin (BNG, ESRI) CODEGEN compilation") { codegenOnly { yMinCodegen(BNGIndexSystem, ESRI) } }
    test("Testing yMin (H3, JTS) CODEGEN_ONLY") { codegenOnly { yMinBehavior(H3IndexSystem, JTS) } }
    test("Testing yMin (H3, ESRI) CODEGEN_ONLY") { codegenOnly { yMinBehavior(H3IndexSystem, ESRI) } }
    test("Testing yMin (BNG, JTS) CODEGEN_ONLY") { codegenOnly { yMinBehavior(BNGIndexSystem, JTS) } }
    test("Testing yMin (BNG, ESRI) CODEGEN_ONLY") { codegenOnly { yMinBehavior(BNGIndexSystem, ESRI) } }

    test("Testing yMax (H3, JTS) NO_CODEGEN") { noCodegen { yMaxBehavior(H3IndexSystem, JTS) } }
    test("Testing yMax (H3, ESRI) NO_CODEGEN") { noCodegen { yMaxBehavior(H3IndexSystem, ESRI) } }
    test("Testing yMax (BNG, JTS) NO_CODEGEN") { noCodegen { yMaxBehavior(BNGIndexSystem, JTS) } }
    test("Testing yMax (BNG, ESRI) NO_CODEGEN") { noCodegen { yMaxBehavior(BNGIndexSystem, ESRI) } }
    test("Testing yMax (H3, JTS) CODEGEN compilation") { codegenOnly { yMaxCodegen(H3IndexSystem, JTS) } }
    test("Testing yMax (H3, ESRI) CODEGEN compilation") { codegenOnly { yMaxCodegen(H3IndexSystem, ESRI) } }
    test("Testing yMax (BNG, JTS) CODEGEN compilation") { codegenOnly { yMaxCodegen(BNGIndexSystem, JTS) } }
    test("Testing yMax (BNG, ESRI) CODEGEN compilation") { codegenOnly { yMaxCodegen(BNGIndexSystem, ESRI) } }
    test("Testing yMax (H3, JTS) CODEGEN_ONLY") { codegenOnly { yMaxBehavior(H3IndexSystem, JTS) } }
    test("Testing yMax (H3, ESRI) CODEGEN_ONLY") { codegenOnly { yMaxBehavior(H3IndexSystem, ESRI) } }
    test("Testing yMax (BNG, JTS) CODEGEN_ONLY") { codegenOnly { yMaxBehavior(BNGIndexSystem, JTS) } }
    test("Testing yMax (BNG, ESRI) CODEGEN_ONLY") { codegenOnly { yMaxBehavior(BNGIndexSystem, ESRI) } }

    test("Testing makeCopy (H3, JTS)") { noCodegen { makeCopy(H3IndexSystem, JTS) } }
    test("Testing makeCopy (H3, ESRI)") { noCodegen { makeCopy(H3IndexSystem, ESRI) } }
    test("Testing makeCopy (BNG, JTS)") { noCodegen { makeCopy(BNGIndexSystem, JTS) } }
    test("Testing makeCopy (BNG, ESRI)") { noCodegen { makeCopy(BNGIndexSystem, ESRI) } }
}
