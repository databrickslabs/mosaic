package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.JTS
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ST_IntersectionTest extends QueryTest with SharedSparkSession with ST_IntersectionBehaviors {

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

    test("Testing stIntersection (H3, JTS) NO_CODEGEN") { noCodegen { intersectionBehaviour(H3IndexSystem, JTS, 7) } }
    test("Testing stIntersection (BNG, JTS) NO_CODEGEN") { noCodegen { intersectionBehaviour(BNGIndexSystem, JTS, 5) } }
    test("Testing self intersection (H3, JTS) NO_CODEGEN") { noCodegen { selfIntersectionBehaviour(H3IndexSystem, JTS, 9) } }
    test("Testing self intersection (BNG, JTS) NO_CODEGEN") { noCodegen { selfIntersectionBehaviour(BNGIndexSystem, JTS, 6) } }
    test("Testing stIntersectionAgg (H3, JTS) NO_CODEGEN") {noCodegen { intersectionAggBehaviour(H3IndexSystem, JTS) }}
    test("Testing stIntersectionAgg (BNG, JTS) NO_CODEGEN") {noCodegen { intersectionAggBehaviour(BNGIndexSystem, JTS) }}
    test("Testing stIntersection (H3, JTS) CODEGEN compilation") { codegenOnly { intersectionCodegen(H3IndexSystem, JTS) } }
    test("Testing stIntersection (BNG, JTS) CODEGEN compilation") { codegenOnly { intersectionCodegen(BNGIndexSystem, JTS) } }
    test("Testing stIntersection auxiliaryMethods (H3, JTS)") { noCodegen { auxiliaryMethods(H3IndexSystem, JTS) } }
    test("Testing stIntersection auxiliaryMethods (BNG, JTS)") { noCodegen { auxiliaryMethods(BNGIndexSystem, JTS) } }

}
