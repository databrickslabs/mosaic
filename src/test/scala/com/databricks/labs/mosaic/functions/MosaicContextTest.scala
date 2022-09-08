package com.databricks.labs.mosaic.functions

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class MosaicContextTest extends QueryTest with SharedSparkSession with MosaicContextBehaviors {

    private val noCodegen =
        withSQLConf(
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
          SQLConf.CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.NO_CODEGEN.toString
        ) _

    test("Testing creation of the context (H3, JTS).") { noCodegen { creationOfContext(H3IndexSystem, JTS) } }
    test("Testing creation of the context (H3, ESRI).") { noCodegen { creationOfContext(H3IndexSystem, ESRI) } }
    test("Testing creation of the context (BNG, JTS).") { noCodegen { creationOfContext(BNGIndexSystem, JTS) } }
    test("Testing creation of the context (BNG, ESRI).") { noCodegen { creationOfContext(BNGIndexSystem, ESRI) } }
    test("Testing sql registration (H3, JTS).") { noCodegen { sqlRegistration(H3IndexSystem, JTS) } }
    test("Testing sql registration (H3, ESRI).") { noCodegen { sqlRegistration(H3IndexSystem, ESRI) } }
    test("Testing sql registration (BNG, JTS).") { noCodegen { sqlRegistration(BNGIndexSystem, JTS) } }
    test("Testing sql registration (BNG, ESRI).") { noCodegen { sqlRegistration(BNGIndexSystem, ESRI) } }

}
