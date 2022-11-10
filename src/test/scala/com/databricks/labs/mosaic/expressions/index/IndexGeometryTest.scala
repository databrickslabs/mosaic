package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class IndexGeometryTest extends QueryTest with SharedSparkSession with IndexGeometryBehaviors {

    private val noCodegen =
        withSQLConf(
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
          SQLConf.CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.NO_CODEGEN.toString
        ) _

    test("Testing auxiliaryMethods (H3, JTS) NO_CODEGEN") { noCodegen { auxiliaryMethods(H3IndexSystem, JTS) } }
    test("Testing auxiliaryMethods (H3, ESRI) NO_CODEGEN") { noCodegen { auxiliaryMethods(H3IndexSystem, ESRI) } }
    test("Testing auxiliaryMethods (BNG, JTS) NO_CODEGEN") { noCodegen { auxiliaryMethods(BNGIndexSystem, JTS) } }
    test("Testing auxiliaryMethods (BNG, ESRI) NO_CODEGEN") { noCodegen { auxiliaryMethods(BNGIndexSystem, ESRI) } }

}
