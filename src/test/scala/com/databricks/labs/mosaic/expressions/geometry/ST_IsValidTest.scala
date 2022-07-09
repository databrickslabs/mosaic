package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ST_IsValidTest extends QueryTest with SharedSparkSession with ST_IsValidBehaviors {

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

    test("Testing stIsValid (H3, JTS) NO_CODEGEN") { noCodegen { isValidBehaviour(H3IndexSystem, JTS) } }
    test("Testing stIsValid (H3, ESRI) NO_CODEGEN") { noCodegen { isValidBehaviour(H3IndexSystem, ESRI) } }
    test("Testing stIsValid (BNG, JTS) NO_CODEGEN") { noCodegen { isValidBehaviour(BNGIndexSystem, JTS) } }
    test("Testing stIsValid (BNG, ESRI) NO_CODEGEN") { noCodegen { isValidBehaviour(BNGIndexSystem, ESRI) } }
    test("Testing stIsValid (H3, JTS) CODEGEN compilation") { codegenOnly { isValidCodegen(H3IndexSystem, JTS) } }
    test("Testing stIsValid (H3, ESRI) CODEGEN compilation") { codegenOnly { isValidCodegen(H3IndexSystem, ESRI) } }
    test("Testing stIsValid (BNG, JTS) CODEGEN compilation") { codegenOnly { isValidCodegen(BNGIndexSystem, JTS) } }
    test("Testing stIsValid (BNG, ESRI) CODEGEN compilation") { codegenOnly { isValidCodegen(BNGIndexSystem, ESRI) } }
    test("Testing stIsValid (H3, JTS) CODEGEN_ONLY") { codegenOnly { isValidBehaviour(H3IndexSystem, JTS) } }
    test("Testing stIsValid (H3, ESRI) CODEGEN_ONLY") { codegenOnly { isValidBehaviour(H3IndexSystem, ESRI) } }
    test("Testing stIsValid (BNG, JTS) CODEGEN_ONLY") { codegenOnly { isValidBehaviour(BNGIndexSystem, JTS) } }
    test("Testing stIsValid (BNG, ESRI) CODEGEN_ONLY") { codegenOnly { isValidBehaviour(BNGIndexSystem, ESRI) } }
    test("Testing stIsValid auxiliaryMethods (H3, JTS)") { noCodegen { auxiliaryMethods(H3IndexSystem, JTS) } }
    test("Testing stIsValid auxiliaryMethods (H3, ESRI)") { noCodegen { auxiliaryMethods(H3IndexSystem, ESRI) } }
    test("Testing stIsValid auxiliaryMethods (BNG, JTS)") { noCodegen { auxiliaryMethods(BNGIndexSystem, JTS) } }
    test("Testing stIsValid auxiliaryMethods (BNG, ESRI)") { noCodegen { auxiliaryMethods(BNGIndexSystem, ESRI) } }

}
