package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ST_ContainsTest extends QueryTest with SharedSparkSession with ST_ContainsBehaviors {

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

    test("Testing stContains (H3, JTS) NO_CODEGEN") { noCodegen { containsBehavior(H3IndexSystem, JTS) } }
    test("Testing stContains (H3, ESRI) NO_CODEGEN") { noCodegen { containsBehavior(H3IndexSystem, ESRI) } }
    test("Testing stContains (BNG, JTS) NO_CODEGEN") { noCodegen { containsBehavior(BNGIndexSystem, JTS) } }
    test("Testing stContains (BNG, ESRI) NO_CODEGEN") { noCodegen { containsBehavior(BNGIndexSystem, ESRI) } }
    test("Testing stContainsAgg (H3, JTS) NO_CODEGEN") { noCodegen { containsAggBehaviour(H3IndexSystem, JTS, 2) } }
    test("Testing stContainsAgg (H3, ESRI) NO_CODEGEN") { noCodegen { containsAggBehaviour(H3IndexSystem, ESRI, 2) } }
    test("Testing stContainsAgg (BNG, JTS) NO_CODEGEN") { noCodegen { containsAggBehaviour(BNGIndexSystem, JTS, 5) } }
    test("Testing stContainsAgg (BNG, ESRI) NO_CODEGEN") { noCodegen { containsAggBehaviour(BNGIndexSystem, ESRI, 5) } }
    test("Testing stContainsMosaic (H3, JTS) NO_CODEGEN") { noCodegen { containsMosaicBehaviour(H3IndexSystem, JTS, 2) } }
    test("Testing stContainsMosaic (H3, ESRI) NO_CODEGEN") { noCodegen { containsMosaicBehaviour(H3IndexSystem, ESRI, 2) } }
    test("Testing stContainsMosaic (BNG, JTS) NO_CODEGEN") { noCodegen { containsMosaicBehaviour(BNGIndexSystem, JTS, 5) } }
    test("Testing stContainsMosaic (BNG, ESRI) NO_CODEGEN") { noCodegen { containsMosaicBehaviour(BNGIndexSystem, ESRI, 5) } }
    test("Testing stContains (H3, JTS) CODEGEN compilation") { codegenOnly { containsCodegen(H3IndexSystem, JTS) } }
    test("Testing stContains (H3, ESRI) CODEGEN compilation") { codegenOnly { containsCodegen(H3IndexSystem, ESRI) } }
    test("Testing stContains (BNG, JTS) CODEGEN compilation") { codegenOnly { containsCodegen(BNGIndexSystem, JTS) } }
    test("Testing stContains (BNG, ESRI) CODEGEN compilation") { codegenOnly { containsCodegen(BNGIndexSystem, ESRI) } }
    test("Testing stContains (H3, JTS) CODEGEN_ONLY") { codegenOnly { containsBehavior(H3IndexSystem, JTS) } }
    test("Testing stContains (H3, ESRI) CODEGEN_ONLY") { codegenOnly { containsBehavior(H3IndexSystem, ESRI) } }
    test("Testing stContains (BNG, JTS) CODEGEN_ONLY") { codegenOnly { containsBehavior(BNGIndexSystem, JTS) } }
    test("Testing stContains (BNG, ESRI) CODEGEN_ONLY") { codegenOnly { containsBehavior(BNGIndexSystem, ESRI) } }
    test("Testing stContains auxiliaryMethods (H3, JTS)") { noCodegen { auxiliaryMethods(H3IndexSystem, JTS) } }
    test("Testing stContains auxiliaryMethods (H3, ESRI)") { noCodegen { auxiliaryMethods(H3IndexSystem, ESRI) } }
    test("Testing stContains auxiliaryMethods (BNG, JTS)") { noCodegen { auxiliaryMethods(BNGIndexSystem, JTS) } }
    test("Testing stContains auxiliaryMethods (BNG, ESRI)") { noCodegen { auxiliaryMethods(BNGIndexSystem, ESRI) } }

}
