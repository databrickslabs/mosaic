package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ST_ScaleTest extends QueryTest with SharedSparkSession with ST_ScaleBehaviors {

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

    test("Testing stScale (H3, JTS) NO_CODEGEN") { noCodegen { scaleBehaviour(H3IndexSystem, JTS) } }
    test("Testing stScale (H3, ESRI) NO_CODEGEN") { noCodegen { scaleBehaviour(H3IndexSystem, ESRI) } }
    test("Testing stScale (BNG, JTS) NO_CODEGEN") { noCodegen { scaleBehaviour(BNGIndexSystem, JTS) } }
    test("Testing stScale (BNG, ESRI) NO_CODEGEN") { noCodegen { scaleBehaviour(BNGIndexSystem, ESRI) } }
    test("Testing stScale (H3, JTS) CODEGEN compilation") { codegenOnly { scaleCodegen(H3IndexSystem, JTS) } }
    test("Testing stScale (H3, ESRI) CODEGEN compilation") { codegenOnly { scaleCodegen(H3IndexSystem, ESRI) } }
    test("Testing stScale (BNG, JTS) CODEGEN compilation") { codegenOnly { scaleCodegen(BNGIndexSystem, JTS) } }
    test("Testing stScale (BNG, ESRI) CODEGEN compilation") { codegenOnly { scaleCodegen(BNGIndexSystem, ESRI) } }
    test("Testing stScale (H3, JTS) CODEGEN_ONLY") { codegenOnly { scaleBehaviour(H3IndexSystem, JTS) } }
    test("Testing stScale (H3, ESRI) CODEGEN_ONLY") { codegenOnly { scaleBehaviour(H3IndexSystem, ESRI) } }
    test("Testing stScale (BNG, JTS) CODEGEN_ONLY") { codegenOnly { scaleBehaviour(BNGIndexSystem, JTS) } }
    test("Testing stScale (BNG, ESRI) CODEGEN_ONLY") { codegenOnly { scaleBehaviour(BNGIndexSystem, ESRI) } }
    test("Testing stScale auxiliaryMethods (H3, JTS)") { noCodegen { auxiliaryMethods(H3IndexSystem, JTS) } }
    test("Testing stScale auxiliaryMethods (H3, ESRI)") { noCodegen { auxiliaryMethods(H3IndexSystem, ESRI) } }
    test("Testing stScale auxiliaryMethods (BNG, JTS)") { noCodegen { auxiliaryMethods(BNGIndexSystem, JTS) } }
    test("Testing stScale auxiliaryMethods (BNG, ESRI)") { noCodegen { auxiliaryMethods(BNGIndexSystem, ESRI) } }

}
