package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ST_LengthTest extends QueryTest with SharedSparkSession with ST_LengthBehaviors {

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

    test("Testing stLength (H3, JTS) NO_CODEGEN") { noCodegen { lengthBehaviour(H3IndexSystem, JTS) } }
    test("Testing stLength (H3, ESRI) NO_CODEGEN") { noCodegen { lengthBehaviour(H3IndexSystem, ESRI) } }
    test("Testing stLength (BNG, JTS) NO_CODEGEN") { noCodegen { lengthBehaviour(BNGIndexSystem, JTS) } }
    test("Testing stLength (BNG, ESRI) NO_CODEGEN") { noCodegen { lengthBehaviour(BNGIndexSystem, ESRI) } }
    test("Testing stLength (H3, JTS) CODEGEN compilation") { codegenOnly { lengthCodegen(H3IndexSystem, JTS) } }
    test("Testing stLength (H3, ESRI) CODEGEN compilation") { codegenOnly { lengthCodegen(H3IndexSystem, ESRI) } }
    test("Testing stLength (BNG, JTS) CODEGEN compilation") { codegenOnly { lengthCodegen(BNGIndexSystem, JTS) } }
    test("Testing stLength (BNG, ESRI) CODEGEN compilation") { codegenOnly { lengthCodegen(BNGIndexSystem, ESRI) } }
    test("Testing stLength (H3, JTS) CODEGEN_ONLY") { codegenOnly { lengthBehaviour(H3IndexSystem, JTS) } }
    test("Testing stLength (H3, ESRI) CODEGEN_ONLY") { codegenOnly { lengthBehaviour(H3IndexSystem, ESRI) } }
    test("Testing stLength (BNG, JTS) CODEGEN_ONLY") { codegenOnly { lengthBehaviour(BNGIndexSystem, JTS) } }
    test("Testing stLength (BNG, ESRI) CODEGEN_ONLY") { codegenOnly { lengthBehaviour(BNGIndexSystem, ESRI) } }
    test("Testing stLength auxiliaryMethods (H3, JTS)") { noCodegen { auxiliaryMethods(H3IndexSystem, JTS) } }
    test("Testing stLength auxiliaryMethods (H3, ESRI)") { noCodegen { auxiliaryMethods(H3IndexSystem, ESRI) } }
    test("Testing stLength auxiliaryMethods (BNG, JTS)") { noCodegen { auxiliaryMethods(BNGIndexSystem, JTS) } }
    test("Testing stLength auxiliaryMethods (BNG, ESRI)") { noCodegen { auxiliaryMethods(BNGIndexSystem, ESRI) } }

}
