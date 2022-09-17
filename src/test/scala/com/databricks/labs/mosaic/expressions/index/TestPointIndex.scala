package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class TestPointIndex extends QueryTest with SharedSparkSession with PointIndexBehaviors {

    private val noCodegen =
        withSQLConf(
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
          SQLConf.CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.NO_CODEGEN.toString
        ) _

    test("Testing stArea (H3, JTS) NO_CODEGEN") { noCodegen { wktPointIndex(H3IndexSystem, JTS, 5) } }
    test("Testing stArea (H3, ESRI) NO_CODEGEN") { noCodegen { wktPointIndex(H3IndexSystem, ESRI, 5) } }
    test("Testing stArea (BNG, JTS) NO_CODEGEN") { noCodegen { wktPointIndex(BNGIndexSystem, JTS, 5) } }
    test("Testing stArea (BNG, ESRI) NO_CODEGEN") { noCodegen { wktPointIndex(BNGIndexSystem, ESRI, 5) } }
    test("Testing stArea auxiliaryMethods (H3, JTS)") { noCodegen { auxiliaryMethods(H3IndexSystem, JTS) } }
    test("Testing stArea auxiliaryMethods (H3, ESRI)") { noCodegen { auxiliaryMethods(H3IndexSystem, ESRI) } }
    test("Testing stArea auxiliaryMethods (BNG, JTS)") { noCodegen { auxiliaryMethods(BNGIndexSystem, JTS) } }
    test("Testing stArea auxiliaryMethods (BNG, ESRI)") { noCodegen { auxiliaryMethods(BNGIndexSystem, ESRI) } }

}
