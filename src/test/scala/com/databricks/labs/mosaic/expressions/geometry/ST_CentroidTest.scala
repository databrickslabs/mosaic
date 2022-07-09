package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ST_CentroidTest extends QueryTest with SharedSparkSession with ST_CentroidBehaviors {

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

    test("Testing stCentroid (H3, JTS) NO_CODEGEN") { noCodegen { centroidBehavior(H3IndexSystem, JTS) } }
    test("Testing stCentroid (H3, ESRI) NO_CODEGEN") { noCodegen { centroidBehavior(H3IndexSystem, ESRI) } }
    test("Testing stCentroid (BNG, JTS) NO_CODEGEN") { noCodegen { centroidBehavior(BNGIndexSystem, JTS) } }
    test("Testing stCentroid (BNG, ESRI) NO_CODEGEN") { noCodegen { centroidBehavior(BNGIndexSystem, ESRI) } }
    test("Testing stCentroid (H3, JTS) CODEGEN compilation") { codegenOnly { centroidCodegen(H3IndexSystem, JTS) } }
    test("Testing stCentroid (H3, ESRI) CODEGEN compilation") { codegenOnly { centroidCodegen(H3IndexSystem, ESRI) } }
    test("Testing stCentroid (BNG, JTS) CODEGEN compilation") { codegenOnly { centroidCodegen(BNGIndexSystem, JTS) } }
    test("Testing stCentroid (BNG, ESRI) CODEGEN compilation") { codegenOnly { centroidCodegen(BNGIndexSystem, ESRI) } }
    test("Testing stCentroid (H3, JTS) CODEGEN_ONLY") { codegenOnly { centroidBehavior(H3IndexSystem, JTS) } }
    test("Testing stCentroid (H3, ESRI) CODEGEN_ONLY") { codegenOnly { centroidBehavior(H3IndexSystem, ESRI) } }
    test("Testing stCentroid (BNG, JTS) CODEGEN_ONLY") { codegenOnly { centroidBehavior(BNGIndexSystem, JTS) } }
    test("Testing stCentroid (BNG, ESRI) CODEGEN_ONLY") { codegenOnly { centroidBehavior(BNGIndexSystem, ESRI) } }
    test("Testing stCentroid auxiliaryMethods (H3, JTS)") { noCodegen { auxiliaryMethods(H3IndexSystem, JTS) } }
    test("Testing stCentroid auxiliaryMethods (H3, ESRI)") { noCodegen { auxiliaryMethods(H3IndexSystem, ESRI) } }
    test("Testing stCentroid auxiliaryMethods (BNG, JTS)") { noCodegen { auxiliaryMethods(BNGIndexSystem, JTS) } }
    test("Testing stCentroid auxiliaryMethods (BNG, ESRI)") { noCodegen { auxiliaryMethods(BNGIndexSystem, ESRI) } }

}
