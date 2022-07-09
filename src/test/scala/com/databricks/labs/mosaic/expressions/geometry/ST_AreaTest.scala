package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ST_AreaTest extends QueryTest with SharedSparkSession with ST_AreaBehaviors {

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

    test("Testing stArea (H3, JTS) NO_CODEGEN") { noCodegen { areaBehavior(H3IndexSystem, JTS) } }
    test("Testing stArea (H3, ESRI) NO_CODEGEN") { noCodegen { areaBehavior(H3IndexSystem, ESRI) } }
    test("Testing stArea (BNG, JTS) NO_CODEGEN") { noCodegen { areaBehavior(BNGIndexSystem, JTS) } }
    test("Testing stArea (BNG, ESRI) NO_CODEGEN") { noCodegen { areaBehavior(BNGIndexSystem, ESRI) } }
    test("Testing stArea (H3, JTS) CODEGEN compilation") { codegenOnly { areaCodegen(H3IndexSystem, JTS) } }
    test("Testing stArea (H3, ESRI) CODEGEN compilation") { codegenOnly { areaCodegen(H3IndexSystem, ESRI) } }
    test("Testing stArea (BNG, JTS) CODEGEN compilation") { codegenOnly { areaCodegen(BNGIndexSystem, JTS) } }
    test("Testing stArea (BNG, ESRI) CODEGEN compilation") { codegenOnly { areaCodegen(BNGIndexSystem, ESRI) } }
    test("Testing stArea (H3, JTS) CODEGEN_ONLY") { codegenOnly { areaBehavior(H3IndexSystem, JTS) } }
    test("Testing stArea (H3, ESRI) CODEGEN_ONLY") { codegenOnly { areaBehavior(H3IndexSystem, ESRI) } }
    test("Testing stArea (BNG, JTS) CODEGEN_ONLY") { codegenOnly { areaBehavior(BNGIndexSystem, JTS) } }
    test("Testing stArea (BNG, ESRI) CODEGEN_ONLY") { codegenOnly { areaBehavior(BNGIndexSystem, ESRI) } }
    test("Testing stArea auxiliaryMethods (H3, JTS)") { noCodegen { auxiliaryMethods(H3IndexSystem, JTS) } }
    test("Testing stArea auxiliaryMethods (H3, ESRI)") { noCodegen { auxiliaryMethods(H3IndexSystem, ESRI) } }
    test("Testing stArea auxiliaryMethods (BNG, JTS)") { noCodegen { auxiliaryMethods(BNGIndexSystem, JTS) } }
    test("Testing stArea auxiliaryMethods (BNG, ESRI)") { noCodegen { auxiliaryMethods(BNGIndexSystem, ESRI) } }

}
