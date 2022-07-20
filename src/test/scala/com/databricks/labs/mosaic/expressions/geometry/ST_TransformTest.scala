package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ST_TransformTest extends QueryTest with SharedSparkSession with ST_TransformBehaviors {

    private val noCodegen =
        withSQLConf(
            SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
            SQLConf.CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.NO_CODEGEN.toString
        ) _

    //test("Testing stTransform assignSRID (H3, JTS) NO_CODEGEN") { noCodegen { assignSRID(H3IndexSystem, JTS) } }
    test("Testing stTransform assignSRID (H3, ESRI) NO_CODEGEN") { noCodegen { assignSRID(H3IndexSystem, ESRI) } }
    test("Testing stTransform assignSRID (BNG, JTS) NO_CODEGEN") { noCodegen { assignSRID(BNGIndexSystem, JTS) } }
    test("Testing stTransform assignSRID (BNG, ESRI) NO_CODEGEN") { noCodegen { assignSRID(BNGIndexSystem, ESRI) } }
    test("Testing stTransform extractSRID (H3, JTS) NO_CODEGEN") { noCodegen { extractSRID(H3IndexSystem, JTS) } }
    test("Testing stTransform extractSRID (H3, ESRI) NO_CODEGEN") { noCodegen { extractSRID(H3IndexSystem, ESRI) } }
    test("Testing stTransform extractSRID (BNG, JTS) NO_CODEGEN") { noCodegen { extractSRID(BNGIndexSystem, JTS) } }
    test("Testing stTransform extractSRID (BNG, ESRI) NO_CODEGEN") { noCodegen { extractSRID(BNGIndexSystem, ESRI) } }
    test("Testing stTransform (H3, JTS) NO_CODEGEN") { noCodegen { reprojectGeometries(H3IndexSystem, JTS) } }
    test("Testing stTransform (H3, ESRI) NO_CODEGEN") { noCodegen { reprojectGeometries(H3IndexSystem, ESRI) } }
    test("Testing stTransform (BNG, JTS) NO_CODEGEN") { noCodegen { reprojectGeometries(BNGIndexSystem, JTS) } }
    test("Testing stTransform (BNG, ESRI) NO_CODEGEN") { noCodegen { reprojectGeometries(BNGIndexSystem, ESRI) } }
    test("Testing stTransform auxiliaryMethods (H3, JTS)") { noCodegen { auxiliaryMethods(H3IndexSystem, JTS) } }
    test("Testing stTransform auxiliaryMethods (H3, ESRI)") { noCodegen { auxiliaryMethods(H3IndexSystem, ESRI) } }
    test("Testing stTransform auxiliaryMethods (BNG, JTS)") { noCodegen { auxiliaryMethods(BNGIndexSystem, JTS) } }
    test("Testing stTransform auxiliaryMethods (BNG, ESRI)") { noCodegen { auxiliaryMethods(BNGIndexSystem, ESRI) } }

}
