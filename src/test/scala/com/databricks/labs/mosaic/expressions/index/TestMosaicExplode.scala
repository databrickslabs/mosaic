package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class TestMosaicExplode extends QueryTest with SharedSparkSession with MosaicExplodeBehaviors {

    private val noCodegen =
        withSQLConf(
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
          SQLConf.CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.NO_CODEGEN.toString
        ) _

    test("Testing wktDecompose (H3, JTS) NO_CODEGEN") { noCodegen { wktDecompose(H3IndexSystem, JTS, 3) } }
    test("Testing wktDecompose (H3, ESRI) NO_CODEGEN") { noCodegen { wktDecompose(H3IndexSystem, ESRI, 3) } }
    test("Testing wktDecompose (BNG, JTS) NO_CODEGEN") { noCodegen { wktDecompose(BNGIndexSystem, JTS, 5) } }
    test("Testing wktDecompose (BNG, ESRI) NO_CODEGEN") { noCodegen { wktDecompose(BNGIndexSystem, ESRI, 5) } }

    test("Testing wktDecomposeKeepCoreParamExpression (H3, JTS) NO_CODEGEN") { noCodegen { wktDecomposeKeepCoreParamExpression(H3IndexSystem, JTS, 3) } }
    test("Testing wktDecomposeKeepCoreParamExpression (H3, ESRI) NO_CODEGEN") { noCodegen { wktDecomposeKeepCoreParamExpression(H3IndexSystem, ESRI, 3) } }
    test("Testing wktDecomposeKeepCoreParamExpression (BNG, JTS) NO_CODEGEN") { noCodegen { wktDecomposeKeepCoreParamExpression(BNGIndexSystem, JTS, 5) } }
    test("Testing wktDecomposeKeepCoreParamExpression (BNG, ESRI) NO_CODEGEN") { noCodegen { wktDecomposeKeepCoreParamExpression(BNGIndexSystem, ESRI, 5) } }

    test("Testing wktDecomposeNoNulls (H3, JTS) NO_CODEGEN") { noCodegen { wktDecomposeNoNulls(H3IndexSystem, JTS, 3) } }
    test("Testing wktDecomposeNoNulls (H3, ESRI) NO_CODEGEN") { noCodegen { wktDecomposeNoNulls(H3IndexSystem, ESRI, 3) } }
    test("Testing wktDecomposeNoNulls (BNG, JTS) NO_CODEGEN") { noCodegen { wktDecomposeNoNulls(BNGIndexSystem, JTS, 5) } }
    test("Testing wktDecomposeNoNulls (BNG, ESRI) NO_CODEGEN") { noCodegen { wktDecomposeNoNulls(BNGIndexSystem, ESRI, 5) } }

    test("Testing wkbDecompose (H3, JTS) NO_CODEGEN") { noCodegen { wkbDecompose(H3IndexSystem, JTS, 3) } }
    test("Testing wkbDecompose (H3, ESRI) NO_CODEGEN") { noCodegen { wkbDecompose(H3IndexSystem, ESRI, 3) } }
    test("Testing wkbDecompose (BNG, JTS) NO_CODEGEN") { noCodegen { wkbDecompose(BNGIndexSystem, JTS, 5) } }
    test("Testing wkbDecompose (BNG, ESRI) NO_CODEGEN") { noCodegen { wkbDecompose(BNGIndexSystem, ESRI, 5) } }

    test("Testing hexDecompose (H3, JTS) NO_CODEGEN") { noCodegen { hexDecompose(H3IndexSystem, JTS, 3) } }
    test("Testing hexDecompose (H3, ESRI) NO_CODEGEN") { noCodegen { hexDecompose(H3IndexSystem, ESRI, 3) } }
    test("Testing hexDecompose (BNG, JTS) NO_CODEGEN") { noCodegen { hexDecompose(BNGIndexSystem, JTS, 5) } }
    test("Testing hexDecompose (BNG, ESRI) NO_CODEGEN") { noCodegen { hexDecompose(BNGIndexSystem, ESRI, 5) } }

    test("Testing coordsDecompose (H3, JTS) NO_CODEGEN") { noCodegen { coordsDecompose(H3IndexSystem, JTS, 3) } }
    test("Testing coordsDecompose (H3, ESRI) NO_CODEGEN") { noCodegen { coordsDecompose(H3IndexSystem, ESRI, 3) } }
    test("Testing coordsDecompose (BNG, JTS) NO_CODEGEN") { noCodegen { coordsDecompose(BNGIndexSystem, JTS, 5) } }
    test("Testing coordsDecompose (BNG, ESRI) NO_CODEGEN") { noCodegen { coordsDecompose(BNGIndexSystem, ESRI, 5) } }

    test("Testing lineDecompose (H3, JTS) NO_CODEGEN") { noCodegen { lineDecompose(H3IndexSystem, JTS, 3) } }
    test("Testing lineDecompose (H3, ESRI) NO_CODEGEN") { noCodegen { lineDecompose(H3IndexSystem, ESRI, 3) } }
    test("Testing lineDecompose (BNG, JTS) NO_CODEGEN") { noCodegen { lineDecompose(BNGIndexSystem, JTS, 3) } }
    test("Testing lineDecompose (BNG, ESRI) NO_CODEGEN") { noCodegen { lineDecompose(BNGIndexSystem, ESRI, 3) } }

    test("Testing lineDecomposeFirstPointOnBoundary (H3, JTS) NO_CODEGEN") { noCodegen { lineDecomposeFirstPointOnBoundary(H3IndexSystem, JTS) } }
    test("Testing lineDecomposeFirstPointOnBoundary (H3, ESRI) NO_CODEGEN") { noCodegen { lineDecomposeFirstPointOnBoundary(H3IndexSystem, ESRI) } }

    test("Testing auxiliaryMethods (H3, JTS) NO_CODEGEN") { noCodegen { auxiliaryMethods(H3IndexSystem, JTS) } }
    test("Testing auxiliaryMethods (H3, ESRI) NO_CODEGEN") { noCodegen { auxiliaryMethods(H3IndexSystem, ESRI) } }
    test("Testing auxiliaryMethods (BNG, JTS) NO_CODEGEN") { noCodegen { auxiliaryMethods(BNGIndexSystem, JTS) } }
    test("Testing auxiliaryMethods (BNG, ESRI) NO_CODEGEN") { noCodegen { auxiliaryMethods(BNGIndexSystem, ESRI) } }

}
