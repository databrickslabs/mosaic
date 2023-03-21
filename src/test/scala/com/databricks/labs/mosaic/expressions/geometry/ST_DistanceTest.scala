package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ST_DistanceTest extends QueryTest with SharedSparkSession with ST_DistanceBehaviors {

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

    test("Testing stDistance (H3, JTS) NO_CODEGEN") { noCodegen { distanceBehavior(H3IndexSystem, JTS) } }
    test("Testing stDistance (H3, ESRI) NO_CODEGEN") { noCodegen { distanceBehavior(H3IndexSystem, ESRI) } }
    test("Testing stDistance (BNG, JTS) NO_CODEGEN") { noCodegen { distanceBehavior(BNGIndexSystem, JTS) } }
    test("Testing stDistance (BNG, ESRI) NO_CODEGEN") { noCodegen { distanceBehavior(BNGIndexSystem, ESRI) } }
    test("Testing stDistance (H3, JTS) CODEGEN compilation") { codegenOnly { distanceCodegen(H3IndexSystem, JTS) } }
    test("Testing stDistance (H3, ESRI) CODEGEN compilation") { codegenOnly { distanceCodegen(H3IndexSystem, ESRI) } }
    test("Testing stDistance (BNG, JTS) CODEGEN compilation") { codegenOnly { distanceCodegen(BNGIndexSystem, JTS) } }
    test("Testing stDistance (BNG, ESRI) CODEGEN compilation") { codegenOnly { distanceCodegen(BNGIndexSystem, ESRI) } }
    test("Testing stDistance (H3, JTS) CODEGEN_ONLY") { codegenOnly { distanceBehavior(H3IndexSystem, JTS) } }
    test("Testing stDistance (H3, ESRI) CODEGEN_ONLY") { codegenOnly { distanceBehavior(H3IndexSystem, ESRI) } }
    test("Testing stDistance (BNG, JTS) CODEGEN_ONLY") { codegenOnly { distanceBehavior(BNGIndexSystem, JTS) } }
    test("Testing stDistance (BNG, ESRI) CODEGEN_ONLY") { codegenOnly { distanceBehavior(BNGIndexSystem, ESRI) } }
    test("Testing stDistance auxiliaryMethods (H3, JTS)") { noCodegen { auxiliaryMethods(H3IndexSystem, JTS) } }
    test("Testing stDistance auxiliaryMethods (H3, ESRI)") { noCodegen { auxiliaryMethods(H3IndexSystem, ESRI) } }
    test("Testing stDistance auxiliaryMethods (BNG, JTS)") { noCodegen { auxiliaryMethods(BNGIndexSystem, JTS) } }
    test("Testing stDistance auxiliaryMethods (BNG, ESRI)") { noCodegen { auxiliaryMethods(BNGIndexSystem, ESRI) } }

}
