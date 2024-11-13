package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.JTS
import com.databricks.labs.mosaic.core.index.H3IndexSystem
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSessionGDAL

import scala.util.Try

class RST_MedianTest extends QueryTest with SharedSparkSessionGDAL with RST_MedianBehaviors {

    private val noCodegen =
        withSQLConf(
            SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
            SQLConf.CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.NO_CODEGEN.toString
        ) _

    // Hotfix for SharedSparkSession afterAll cleanup.
    override def afterAll(): Unit = Try(super.afterAll())

    // These tests are not index system nor geometry API specific.
    // Only testing one pairing is sufficient.
    test("Testing rst_median behavior with H3IndexSystem and JTS (tessellation case)") {
        noCodegen {
            assume(System.getProperty("os.name") == "Linux")
            largeAreaBehavior(H3IndexSystem, JTS)
        }
    }

        test("Testing rst_median behavior with H3IndexSystem and JTS (small area case)") {
            noCodegen {
                assume(System.getProperty("os.name") == "Linux")
                smallAreaBehavior(H3IndexSystem, JTS)
            }
    }

}
