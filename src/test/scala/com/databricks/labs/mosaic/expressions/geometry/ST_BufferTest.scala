package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ST_BufferTest extends QueryTest with SharedSparkSession with ST_BufferBehaviors {

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

    test("Testing stBuffer (H3, JTS) NO_CODEGEN") { noCodegen { bufferBehavior(H3IndexSystem, JTS) } }
    test("Testing stBuffer (H3, ESRI) NO_CODEGEN") { noCodegen { bufferBehavior(H3IndexSystem, ESRI) } }
    test("Testing stBuffer (BNG, JTS) NO_CODEGEN") { noCodegen { bufferBehavior(BNGIndexSystem, JTS) } }
    test("Testing stBuffer (BNG, ESRI) NO_CODEGEN") { noCodegen { bufferBehavior(BNGIndexSystem, ESRI) } }
    test("Testing stBuffer (H3, JTS) CODEGEN compilation") { codegenOnly { bufferCodegen(H3IndexSystem, JTS) } }
    test("Testing stBuffer (H3, ESRI) CODEGEN compilation") { codegenOnly { bufferCodegen(H3IndexSystem, ESRI) } }
    test("Testing stBuffer (BNG, JTS) CODEGEN compilation") { codegenOnly { bufferCodegen(BNGIndexSystem, JTS) } }
    test("Testing stBuffer (BNG, ESRI) CODEGEN compilation") { codegenOnly { bufferCodegen(BNGIndexSystem, ESRI) } }
    test("Testing stBuffer (H3, JTS) CODEGEN_ONLY") { codegenOnly { bufferBehavior(H3IndexSystem, JTS) } }
    test("Testing stBuffer (H3, ESRI) CODEGEN_ONLY") { codegenOnly { bufferBehavior(H3IndexSystem, ESRI) } }
    test("Testing stBuffer (BNG, JTS) CODEGEN_ONLY") { codegenOnly { bufferBehavior(BNGIndexSystem, JTS) } }
    test("Testing stBuffer (BNG, ESRI) CODEGEN_ONLY") { codegenOnly { bufferBehavior(BNGIndexSystem, ESRI) } }
    test("Testing stBuffer auxiliaryMethods (H3, JTS)") { noCodegen { auxiliaryMethods(H3IndexSystem, JTS) } }
    test("Testing stBuffer auxiliaryMethods (H3, ESRI)") { noCodegen { auxiliaryMethods(H3IndexSystem, ESRI) } }
    test("Testing stBuffer auxiliaryMethods (BNG, JTS)") { noCodegen { auxiliaryMethods(BNGIndexSystem, JTS) } }
    test("Testing stBuffer auxiliaryMethods (BNG, ESRI)") { noCodegen { auxiliaryMethods(BNGIndexSystem, ESRI) } }

}
