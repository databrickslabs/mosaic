package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ST_HasValidCoordinatesTest extends QueryTest with SharedSparkSession with ST_HasValidCoordinatesBehaviors {

    private val noCodegen =
        withSQLConf(
            SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
            SQLConf.CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.NO_CODEGEN.toString
        ) _

    test("Testing stHasValidCoords (H3, JTS) wkt NO_CODEGEN") { noCodegen { hasValidCoordinatesBehaviours(H3IndexSystem, JTS) } }
    test("Testing stHasValidCoords (H3, ESRI) wkt NO_CODEGEN") { noCodegen { hasValidCoordinatesBehaviours(H3IndexSystem, ESRI) } }
    test("Testing stHasValidCoords (BNG, JTS) wkt NO_CODEGEN") { noCodegen { hasValidCoordinatesBehaviours(BNGIndexSystem, JTS) } }
    test("Testing stHasValidCoords (BNG, ESRI) wkt NO_CODEGEN") { noCodegen { hasValidCoordinatesBehaviours(BNGIndexSystem, ESRI) } }
    test("Testing stHasValidCoords auxiliaryMethods (H3, JTS)") { noCodegen { auxiliaryMethods(H3IndexSystem, JTS) } }
    test("Testing stHasValidCoords auxiliaryMethods (H3, ESRI)") { noCodegen { auxiliaryMethods(H3IndexSystem, ESRI) } }
    test("Testing stHasValidCoords auxiliaryMethods (BNG, JTS)") { noCodegen { auxiliaryMethods(BNGIndexSystem, JTS) } }
    test("Testing stHasValidCoords auxiliaryMethods (BNG, ESRI)") { noCodegen { auxiliaryMethods(BNGIndexSystem, ESRI) } }

}
