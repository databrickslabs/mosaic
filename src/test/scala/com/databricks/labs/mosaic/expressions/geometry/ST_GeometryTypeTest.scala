package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ST_GeometryTypeTest extends QueryTest with SharedSparkSession with ST_GeometryTypeBehaviors {

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

    test("Testing stGeometryType (H3, JTS) wkt NO_CODEGEN") { noCodegen { wktTypeBehavior(H3IndexSystem, JTS) } }
    test("Testing stGeometryType (H3, ESRI) wkt NO_CODEGEN") { noCodegen { wktTypeBehavior(H3IndexSystem, ESRI) } }
    test("Testing stGeometryType (BNG, JTS) wkt NO_CODEGEN") { noCodegen { wktTypeBehavior(BNGIndexSystem, JTS) } }
    test("Testing stGeometryType (BNG, ESRI) wkt NO_CODEGEN") { noCodegen { wktTypeBehavior(BNGIndexSystem, ESRI) } }
    test("Testing stGeometryType (H3, JTS) wkt CODEGEN compilation") { codegenOnly { wktTypesCodegen(H3IndexSystem, JTS) } }
    test("Testing stGeometryType (H3, ESRI) wkt CODEGEN compilation") { codegenOnly { wktTypesCodegen(H3IndexSystem, ESRI) } }
    test("Testing stGeometryType (BNG, JTS) wkt CODEGEN compilation") { codegenOnly { wktTypesCodegen(BNGIndexSystem, JTS) } }
    test("Testing stGeometryType (BNG, ESRI) wkt CODEGEN compilation") { codegenOnly { wktTypesCodegen(BNGIndexSystem, ESRI) } }
    test("Testing stGeometryType (H3, JTS) hex CODEGEN_ONLY") { codegenOnly { hexTypesBehavior(H3IndexSystem, JTS) } }
    test("Testing stGeometryType (H3, ESRI) hex CODEGEN_ONLY") { codegenOnly { hexTypesBehavior(H3IndexSystem, ESRI) } }
    test("Testing stGeometryType (BNG, JTS) hex CODEGEN_ONLY") { codegenOnly { hexTypesBehavior(BNGIndexSystem, JTS) } }
    test("Testing stGeometryType (BNG, ESRI) hex CODEGEN_ONLY") { codegenOnly { hexTypesBehavior(BNGIndexSystem, ESRI) } }
    test("Testing stGeometryType (H3, JTS) hex CODEGEN compilation") { codegenOnly { hexTypesCodegen(H3IndexSystem, JTS) } }
    test("Testing stGeometryType (H3, ESRI) hex CODEGEN compilation") { codegenOnly { hexTypesCodegen(H3IndexSystem, ESRI) } }
    test("Testing stGeometryType (BNG, JTS) hex CODEGEN compilation") { codegenOnly { hexTypesCodegen(BNGIndexSystem, JTS) } }
    test("Testing stGeometryType (BNG, ESRI) hex CODEGEN compilation") { codegenOnly { hexTypesCodegen(BNGIndexSystem, ESRI) } }
    test("Testing stGeometryType auxiliaryMethods (H3, JTS)") { noCodegen { auxiliaryMethods(H3IndexSystem, JTS) } }
    test("Testing stGeometryType auxiliaryMethods (H3, ESRI)") { noCodegen { auxiliaryMethods(H3IndexSystem, ESRI) } }
    test("Testing stGeometryType auxiliaryMethods (BNG, JTS)") { noCodegen { auxiliaryMethods(BNGIndexSystem, JTS) } }
    test("Testing stGeometryType auxiliaryMethods (BNG, ESRI)") { noCodegen { auxiliaryMethods(BNGIndexSystem, ESRI) } }

}
