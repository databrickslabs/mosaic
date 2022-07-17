package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ST_IntersectsTest extends QueryTest with SharedSparkSession with ST_IntersectsBehaviors {

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

    test("Testing stIntersects (H3, JTS) NO_CODEGEN") { noCodegen { intersectsBehaviour(H3IndexSystem, JTS, 7) } }
    test("Testing stIntersects (H3, ESRI) NO_CODEGEN") { noCodegen { intersectsBehaviour(H3IndexSystem, ESRI, 7) } }
    test("Testing stIntersects (BNG, JTS) NO_CODEGEN") { noCodegen { intersectsBehaviour(BNGIndexSystem, JTS, 5) } }
    test("Testing stIntersects (BNG, ESRI) NO_CODEGEN") {noCodegen { intersectsBehaviour(BNGIndexSystem, ESRI, 5) } }
    test("Testing stIntersects self intersection (H3, JTS) NO_CODEGEN") {
        noCodegen { selfIntersectsBehaviour(H3IndexSystem, JTS, 9) }
    }
    test("Testing stIntersects self intersection (H3, ESRI) NO_CODEGEN") {
        noCodegen { selfIntersectsBehaviour(H3IndexSystem, ESRI, 9) }
    }
    test("Testing stIntersects self intersection (BNG, JTS) NO_CODEGEN") {
        noCodegen { selfIntersectsBehaviour(BNGIndexSystem, JTS, 6) }
    }
    test("Testing stIntersects self intersection (BNG, ESRI) NO_CODEGEN") {
        noCodegen { selfIntersectsBehaviour(BNGIndexSystem, ESRI, 6) }
    }
    test("Testing stIntersects (H3, JTS) CODEGEN compilation") { codegenOnly { intersectsCodegen(H3IndexSystem, JTS) } }
    test("Testing stIntersects (H3, ESRI) CODEGEN compilation") { codegenOnly { intersectsCodegen(H3IndexSystem, ESRI) } }
    test("Testing stIntersects (BNG, JTS) CODEGEN compilation") { codegenOnly { intersectsCodegen(BNGIndexSystem, JTS) } }
    test("Testing stIntersects (BNG, ESRI) CODEGEN compilation") { codegenOnly { intersectsCodegen(BNGIndexSystem, ESRI) } }
    test("Testing stIntersects auxiliaryMethods (H3, JTS)") { noCodegen { auxiliaryMethods(H3IndexSystem, JTS) } }
    test("Testing stIntersects auxiliaryMethods (H3, ESRI)") { noCodegen { auxiliaryMethods(H3IndexSystem, ESRI) } }
    test("Testing stIntersects auxiliaryMethods (BNG, JTS)") { noCodegen { auxiliaryMethods(BNGIndexSystem, JTS) } }
    test("Testing stIntersects auxiliaryMethods (BNG, ESRI)") { noCodegen { auxiliaryMethods(BNGIndexSystem, ESRI) } }

}
