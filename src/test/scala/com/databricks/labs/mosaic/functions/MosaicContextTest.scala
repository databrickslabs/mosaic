package com.databricks.labs.mosaic.functions

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

import scala.util.Try

class MosaicContextTest extends QueryTest with SharedSparkSession with MosaicContextBehaviors {

    //Hotfix for SharedSparkSession afterAll cleanup.
    override def afterAll(): Unit = Try(super.afterAll())

    private val noCodegen =
        withSQLConf(
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
          SQLConf.CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.NO_CODEGEN.toString
        ) _

    test("Testing sql registration (H3, JTS).") { noCodegen { sqlRegistration(H3IndexSystem, JTS) } }
    test("Testing sql registration (H3, ESRI).") { noCodegen { sqlRegistration(H3IndexSystem, ESRI) } }
    test("Testing sql registration (BNG, JTS).") { noCodegen { sqlRegistration(BNGIndexSystem, JTS) } }
    test("Testing sql registration (BNG, ESRI).") { noCodegen { sqlRegistration(BNGIndexSystem, ESRI) } }

}
