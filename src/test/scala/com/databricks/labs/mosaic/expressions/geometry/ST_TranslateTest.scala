package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.JTS
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ST_TranslateTest extends QueryTest with SharedSparkSession with ST_TranslateBehaviors {

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

    test("Testing stTranslate (H3, JTS) NO_CODEGEN") { noCodegen { translateBehaviour(H3IndexSystem, JTS) } }
    test("Testing stTranslate (BNG, JTS) NO_CODEGEN") { noCodegen { translateBehaviour(BNGIndexSystem, JTS) } }
    test("Testing stTranslate (H3, JTS) CODEGEN compilation") { codegenOnly { translateCodegen(H3IndexSystem, JTS) } }
    test("Testing stTranslate (BNG, JTS) CODEGEN compilation") { codegenOnly { translateCodegen(BNGIndexSystem, JTS) } }
    test("Testing stTranslate (H3, JTS) CODEGEN_ONLY") { codegenOnly { translateBehaviour(H3IndexSystem, JTS) } }
    test("Testing stTranslate (BNG, JTS) CODEGEN_ONLY") { codegenOnly { translateBehaviour(BNGIndexSystem, JTS) } }
    test("Testing stTranslate auxiliaryMethods (H3, JTS)") { noCodegen { auxiliaryMethods(H3IndexSystem, JTS) } }
    test("Testing stTranslate auxiliaryMethods (BNG, JTS)") { noCodegen { auxiliaryMethods(BNGIndexSystem, JTS) } }

}
