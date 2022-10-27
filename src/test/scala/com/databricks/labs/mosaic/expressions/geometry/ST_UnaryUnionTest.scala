package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.index.H3IndexSystem
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem, IndexSystem}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.{MockIndexSystem, MosaicSpatialQueryTest, SparkSuite}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ST_UnaryUnionTest extends MosaicSpatialQueryTest with SharedSparkSession with ST_UnaryUnionBehaviours {

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

    testAllGeometriesAllIndexSystems("ST_UnaryUnion") { uub }

//    test("Testing stUnaryUnion (JTS) NO_CODEGEN") { noCodegen(uub(MosaicContext.build(H3IndexSystem, ESRI))) }
//    test("Testing stUnaryUnion (ESRI) NO_CODEGEN") { noCodegen { unaryUnionBehavior(MockIndexSystem, ESRI) } }
//    test("Testing stUnaryUnion (JTS) CODEGEN compilation") { codegenOnly { unaryUnionCodegen(MockIndexSystem, JTS) } }
//    test("Testing stUnaryUnion (ESRI) CODEGEN compilation") { codegenOnly { unaryUnionCodegen(MockIndexSystem, ESRI) } }
//    test("Testing stUnaryUnion (JTS) CODEGEN_ONLY") { codegenOnly { unaryUnionBehavior(MockIndexSystem, JTS) } }
//    test("Testing stUnaryUnion (ESRI) CODEGEN_ONLY") { codegenOnly { unaryUnionBehavior(MockIndexSystem, ESRI) } }
//    test("Testing stUnaryUnion auxiliaryMethods (JTS)") { noCodegen { auxiliaryMethods(MockIndexSystem, JTS) } }
//    test("Testing stUnaryUnion auxiliaryMethods (ESRI)") { noCodegen { auxiliaryMethods(MockIndexSystem, ESRI) } }

}
