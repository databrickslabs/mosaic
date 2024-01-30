package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.JTS
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ST_ConcaveHullTest extends QueryTest with SharedSparkSession with ST_ConcaveHullBehaviors {

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

    test("Testing ST_ConcaveHull (H3, JTS) NO_CODEGEN") { noCodegen { concaveHullBehavior(H3IndexSystem, JTS) } }
    test("Testing ST_ConcaveHull (BNG, JTS) NO_CODEGEN") { noCodegen { concaveHullBehavior(BNGIndexSystem, JTS) } }
    test("Testing ST_ConcaveHull (H3, JTS) CODEGEN compilation") { codegenOnly { concaveHullCodegen(H3IndexSystem, JTS) } }
    test("Testing ST_ConcaveHull (BNG, JTS) CODEGEN compilation") { codegenOnly { concaveHullCodegen(BNGIndexSystem, JTS) } }
    test("Testing ST_ConcaveHull (H3, JTS) CODEGEN_ONLY") { codegenOnly { concaveHullBehavior(H3IndexSystem, JTS) } }
    test("Testing ST_ConcaveHull (BNG, JTS) CODEGEN_ONLY") { codegenOnly { concaveHullBehavior(BNGIndexSystem, JTS) } }
    test("Testing ST_ConcaveHull auxiliaryMethods (H3, JTS)") { noCodegen { auxiliaryMethods(H3IndexSystem, JTS) } }
    test("Testing ST_ConcaveHull auxiliaryMethods (BNG, JTS)") { noCodegen { auxiliaryMethods(BNGIndexSystem, JTS) } }

}
