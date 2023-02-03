package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class FlattenPolygonsTest extends QueryTest with SharedSparkSession with FlattenPolygonsBehaviors {

    private val noCodegen =
        withSQLConf(
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
          SQLConf.CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.NO_CODEGEN.toString
        ) _

    test("Testing flattenWKBPolygon (H3, JTS) NO_CODEGEN") { noCodegen { flattenWKBPolygon(H3IndexSystem, JTS) } }
    test("Testing flattenWKBPolygon (H3, ESRI) NO_CODEGEN") { noCodegen { flattenWKBPolygon(H3IndexSystem, ESRI) } }
    test("Testing flattenWKBPolygon (BNG, JTS) NO_CODEGEN") { noCodegen { flattenWKBPolygon(BNGIndexSystem, JTS) } }
    test("Testing flattenWKBPolygon (BNG, ESRI) NO_CODEGEN") { noCodegen { flattenWKBPolygon(BNGIndexSystem, ESRI) } }

    test("Testing flattenWKTPolygon (H3, JTS) NO_CODEGEN") { noCodegen { flattenWKTPolygon(H3IndexSystem, JTS) } }
    test("Testing flattenWKTPolygon (H3, ESRI) NO_CODEGEN") { noCodegen { flattenWKTPolygon(H3IndexSystem, ESRI) } }
    test("Testing flattenWKTPolygon (BNG, JTS) NO_CODEGEN") { noCodegen { flattenWKTPolygon(BNGIndexSystem, JTS) } }
    test("Testing flattenWKTPolygon (BNG, ESRI) NO_CODEGEN") { noCodegen { flattenWKTPolygon(BNGIndexSystem, ESRI) } }

    test("Testing flattenCOORDSPolygon (H3, JTS) NO_CODEGEN") { noCodegen { flattenCOORDSPolygon(H3IndexSystem, JTS) } }
    test("Testing flattenCOORDSPolygon (H3, ESRI) NO_CODEGEN") { noCodegen { flattenCOORDSPolygon(H3IndexSystem, ESRI) } }
    test("Testing flattenCOORDSPolygon (BNG, JTS) NO_CODEGEN") { noCodegen { flattenCOORDSPolygon(BNGIndexSystem, JTS) } }
    test("Testing flattenCOORDSPolygon (BNG, ESRI) NO_CODEGEN") { noCodegen { flattenCOORDSPolygon(BNGIndexSystem, ESRI) } }

    test("Testing flattenHEXPolygon (H3, JTS) NO_CODEGEN") { noCodegen { flattenHEXPolygon(H3IndexSystem, JTS) } }
    test("Testing flattenHEXPolygon (H3, ESRI) NO_CODEGEN") { noCodegen { flattenHEXPolygon(H3IndexSystem, ESRI) } }
    test("Testing flattenHEXPolygon (BNG, JTS) NO_CODEGEN") { noCodegen { flattenHEXPolygon(BNGIndexSystem, JTS) } }
    test("Testing flattenHEXPolygon (BNG, ESRI) NO_CODEGEN") { noCodegen { flattenHEXPolygon(BNGIndexSystem, ESRI) } }

    test("Testing flattenPolygons failDataTypeCheck (H3, JTS)") { noCodegen { failDataTypeCheck(H3IndexSystem, JTS) } }
    test("Testing flattenPolygons failDataTypeCheck (H3, ESRI)") { noCodegen { failDataTypeCheck(H3IndexSystem, ESRI) } }
    test("Testing flattenPolygons failDataTypeCheck (BNG, JTS)") { noCodegen { failDataTypeCheck(BNGIndexSystem, JTS) } }
    test("Testing flattenPolygons failDataTypeCheck (BNG, ESRI)") { noCodegen { failDataTypeCheck(BNGIndexSystem, ESRI) } }

    test("Testing flattenPolygons auxiliaryMethods (H3, JTS)") { noCodegen { auxiliaryMethods(H3IndexSystem, JTS) } }
    test("Testing flattenPolygons auxiliaryMethods (H3, ESRI)") { noCodegen { auxiliaryMethods(H3IndexSystem, ESRI) } }
    test("Testing flattenPolygons auxiliaryMethods (BNG, JTS)") { noCodegen { auxiliaryMethods(BNGIndexSystem, JTS) } }
    test("Testing flattenPolygons auxiliaryMethods (BNG, ESRI)") { noCodegen { auxiliaryMethods(BNGIndexSystem, ESRI) } }

}
