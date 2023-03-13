package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ST_UnionTest extends QueryTest with SharedSparkSession with ST_UnionBehaviors {

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

    test("Testing stUnion (H3, JTS) NO_CODEGEN") { noCodegen { unionBehavior(H3IndexSystem, JTS) } }
    test("Testing stUnion (H3, ESRI) NO_CODEGEN") { noCodegen { unionBehavior(H3IndexSystem, ESRI) } }
    test("Testing stUnion (BNG, JTS) NO_CODEGEN") { noCodegen { unionBehavior(BNGIndexSystem, JTS) } }
    test("Testing stUnion (BNG, ESRI) NO_CODEGEN") { noCodegen { unionBehavior(BNGIndexSystem, ESRI) } }
    test("Testing stUnionAgg (H3, JTS) NO_CODEGEN") { noCodegen { unionAggBehavior(H3IndexSystem, JTS) } }
    test("Testing stUnionAgg (H3, ESRI) NO_CODEGEN") { noCodegen { unionAggBehavior(H3IndexSystem, ESRI) } }
    test("Testing stUnionAgg (BNG, JTS) NO_CODEGEN") { noCodegen { unionAggBehavior(BNGIndexSystem, JTS) } }
    test("Testing stUnionAgg (BNG, ESRI) NO_CODEGEN") { noCodegen { unionAggBehavior(BNGIndexSystem, ESRI) } }
    test("Testing stUnion (H3, JTS) CODEGEN compilation") { codegenOnly { unionCodegen(H3IndexSystem, JTS) } }
    test("Testing stUnion (H3, ESRI) CODEGEN compilation") { codegenOnly { unionCodegen(H3IndexSystem, ESRI) } }
    test("Testing stUnion (BNG, JTS) CODEGEN compilation") { codegenOnly { unionCodegen(BNGIndexSystem, JTS) } }
    test("Testing stUnion (BNG, ESRI) CODEGEN compilation") { codegenOnly { unionCodegen(BNGIndexSystem, ESRI) } }
    test("Testing stUnion (H3, JTS) CODEGEN_ONLY") { codegenOnly { unionBehavior(H3IndexSystem, JTS) } }
    test("Testing stUnion (H3, ESRI) CODEGEN_ONLY") { codegenOnly { unionBehavior(H3IndexSystem, ESRI) } }
    test("Testing stUnion (BNG, JTS) CODEGEN_ONLY") { codegenOnly { unionBehavior(BNGIndexSystem, JTS) } }
    test("Testing stUnion (BNG, ESRI) CODEGEN_ONLY") { codegenOnly { unionBehavior(BNGIndexSystem, ESRI) } }
    test("Testing stUnion auxiliaryMethods (H3, JTS)") { noCodegen { auxiliaryMethods(H3IndexSystem, JTS) } }
    test("Testing stUnion auxiliaryMethods (H3, ESRI)") { noCodegen { auxiliaryMethods(H3IndexSystem, ESRI) } }
    test("Testing stUnion auxiliaryMethods (BNG, JTS)") { noCodegen { auxiliaryMethods(BNGIndexSystem, JTS) } }
    test("Testing stUnion auxiliaryMethods (BNG, ESRI)") { noCodegen { auxiliaryMethods(BNGIndexSystem, ESRI) } }

}
