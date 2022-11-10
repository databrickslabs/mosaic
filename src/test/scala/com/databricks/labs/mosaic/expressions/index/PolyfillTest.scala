package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

class PolyfillTest extends QueryTest with SharedSparkSession with PolyfillBehaviors {

    private val noCodegen =
        withSQLConf(
            SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
            SQLConf.CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.NO_CODEGEN.toString
        ) _

    test("Testing polyfillOnComputedColumns (H3, JTS) NO_CODEGEN") { noCodegen { polyfillOnComputedColumns(H3IndexSystem, JTS, 11) } }
    test("Testing polyfillOnComputedColumns (H3, ESRI) NO_CODEGEN") { noCodegen { polyfillOnComputedColumns(H3IndexSystem, ESRI, 11) } }
    test("Testing polyfillOnComputedColumns (BNG, JTS) NO_CODEGEN") { noCodegen { polyfillOnComputedColumns(BNGIndexSystem, JTS, 4) } }
    test("Testing polyfillOnComputedColumns (BNG, ESRI) NO_CODEGEN") { noCodegen { polyfillOnComputedColumns(BNGIndexSystem, ESRI, 4) } }

    test("Testing wktPolyfill (H3, JTS) NO_CODEGEN") { noCodegen { wktPolyfill(H3IndexSystem, JTS, 11) } }
    test("Testing wktPolyfill (H3, ESRI) NO_CODEGEN") { noCodegen { wktPolyfill(H3IndexSystem, ESRI, 11) } }
    test("Testing wktPolyfill (BNG, JTS) NO_CODEGEN") { noCodegen { wktPolyfill(BNGIndexSystem, JTS, 4) } }
    test("Testing wktPolyfill (BNG, ESRI) NO_CODEGEN") { noCodegen { wktPolyfill(BNGIndexSystem, ESRI, 4) } }

    test("Testing wkbPolyfill (H3, JTS) NO_CODEGEN") { noCodegen { wkbPolyfill(H3IndexSystem, JTS, 11) } }
    test("Testing wkbPolyfill (H3, ESRI) NO_CODEGEN") { noCodegen { wkbPolyfill(H3IndexSystem, ESRI, 11) } }
    test("Testing wkbPolyfill (BNG, JTS) NO_CODEGEN") { noCodegen { wkbPolyfill(BNGIndexSystem, JTS, 4) } }
    test("Testing wkbPolyfill (BNG, ESRI) NO_CODEGEN") { noCodegen { wkbPolyfill(BNGIndexSystem, ESRI, 4) } }

    test("Testing hexPolyfill (H3, JTS) NO_CODEGEN") { noCodegen { hexPolyfill(H3IndexSystem, JTS, 11) } }
    test("Testing hexPolyfill (H3, ESRI) NO_CODEGEN") { noCodegen { hexPolyfill(H3IndexSystem, ESRI, 11) } }
    test("Testing hexPolyfill (BNG, JTS) NO_CODEGEN") { noCodegen { hexPolyfill(BNGIndexSystem, JTS, 4) } }
    test("Testing hexPolyfill (BNG, ESRI) NO_CODEGEN") { noCodegen { hexPolyfill(BNGIndexSystem, ESRI, 4) } }

    test("Testing coordsPolyfill (H3, JTS) NO_CODEGEN") { noCodegen { coordsPolyfill(H3IndexSystem, JTS, 11) } }
    test("Testing coordsPolyfill (H3, ESRI) NO_CODEGEN") { noCodegen { coordsPolyfill(H3IndexSystem, ESRI, 11) } }
    test("Testing coordsPolyfill (BNG, JTS) NO_CODEGEN") { noCodegen { coordsPolyfill(BNGIndexSystem, JTS, 4) } }
    test("Testing coordsPolyfill (BNG, ESRI) NO_CODEGEN") { noCodegen { coordsPolyfill(BNGIndexSystem, ESRI, 4) } }

    test("Testing auxiliaryMethods (H3, JTS) NO_CODEGEN") { noCodegen { auxiliaryMethods(H3IndexSystem, JTS) } }
    test("Testing auxiliaryMethods (H3, ESRI) NO_CODEGEN") { noCodegen { auxiliaryMethods(H3IndexSystem, ESRI) } }
    test("Testing auxiliaryMethods (BNG, JTS) NO_CODEGEN") { noCodegen { auxiliaryMethods(BNGIndexSystem, JTS) } }
    test("Testing auxiliaryMethods (BNG, ESRI) NO_CODEGEN") { noCodegen { auxiliaryMethods(BNGIndexSystem, ESRI) } }

}
