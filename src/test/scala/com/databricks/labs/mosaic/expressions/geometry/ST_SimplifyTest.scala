package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ST_SimplifyTest extends QueryTest with SharedSparkSession with ST_SimplifyBehaviors {

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

    test("Testing stSimplify (H3, JTS) NO_CODEGEN") { noCodegen { simplifyBehavior(H3IndexSystem, JTS) } }
    test("Testing stSimplify (H3, ESRI) NO_CODEGEN") { noCodegen { simplifyBehavior(H3IndexSystem, ESRI) } }
    test("Testing stSimplify (H3, JTS) CODEGEN compilation") { codegenOnly { simplifyCodegen(H3IndexSystem, JTS) } }
    test("Testing stSimplify (H3, ESRI) CODEGEN compilation") { codegenOnly { simplifyCodegen(H3IndexSystem, ESRI) } }
    test("Testing stSimplify (H3, JTS) CODEGEN_ONLY") { codegenOnly { simplifyBehavior(H3IndexSystem, JTS) } }
    test("Testing stSimplify (H3, ESRI) CODEGEN_ONLY") { codegenOnly { simplifyBehavior(H3IndexSystem, ESRI) } }
    test("Testing stSimplify auxiliaryMethods (H3, JTS)") { noCodegen { auxiliaryMethods(H3IndexSystem, JTS) } }
    test("Testing stSimplify auxiliaryMethods (H3, ESRI)") { noCodegen { auxiliaryMethods(H3IndexSystem, ESRI) } }

}
