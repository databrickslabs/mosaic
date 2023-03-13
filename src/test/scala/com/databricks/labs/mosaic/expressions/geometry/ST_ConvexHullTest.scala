package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ST_ConvexHullTest extends QueryTest with SharedSparkSession with ST_ConvexHullBehaviors {

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

    test("Testing stConvexHull (H3, JTS) NO_CODEGEN") { noCodegen { convexHullBehavior(H3IndexSystem, JTS) } }
    test("Testing stConvexHull (H3, ESRI) NO_CODEGEN") { noCodegen { convexHullBehavior(H3IndexSystem, ESRI) } }
    test("Testing stConvexHull (BNG, JTS) NO_CODEGEN") { noCodegen { convexHullBehavior(BNGIndexSystem, JTS) } }
    test("Testing stConvexHull (BNG, ESRI) NO_CODEGEN") { noCodegen { convexHullBehavior(BNGIndexSystem, ESRI) } }
    test("Testing stConvexHull (H3, JTS) CODEGEN compilation") { codegenOnly { convexHullCodegen(H3IndexSystem, JTS) } }
    test("Testing stConvexHull (H3, ESRI) CODEGEN compilation") { codegenOnly { convexHullCodegen(H3IndexSystem, ESRI) } }
    test("Testing stConvexHull (BNG, JTS) CODEGEN compilation") { codegenOnly { convexHullCodegen(BNGIndexSystem, JTS) } }
    test("Testing stConvexHull (BNG, ESRI) CODEGEN compilation") { codegenOnly { convexHullCodegen(BNGIndexSystem, ESRI) } }
    test("Testing stConvexHull (H3, JTS) CODEGEN_ONLY") { codegenOnly { convexHullBehavior(H3IndexSystem, JTS) } }
    test("Testing stConvexHull (H3, ESRI) CODEGEN_ONLY") { codegenOnly { convexHullBehavior(H3IndexSystem, ESRI) } }
    test("Testing stConvexHull (BNG, JTS) CODEGEN_ONLY") { codegenOnly { convexHullBehavior(BNGIndexSystem, JTS) } }
    test("Testing stConvexHull (BNG, ESRI) CODEGEN_ONLY") { codegenOnly { convexHullBehavior(BNGIndexSystem, ESRI) } }
    test("Testing stConvexHull auxiliaryMethods (H3, JTS)") { noCodegen { auxiliaryMethods(H3IndexSystem, JTS) } }
    test("Testing stConvexHull auxiliaryMethods (H3, ESRI)") { noCodegen { auxiliaryMethods(H3IndexSystem, ESRI) } }
    test("Testing stConvexHull auxiliaryMethods (BNG, JTS)") { noCodegen { auxiliaryMethods(BNGIndexSystem, JTS) } }
    test("Testing stConvexHull auxiliaryMethods (BNG, ESRI)") { noCodegen { auxiliaryMethods(BNGIndexSystem, ESRI) } }

}
