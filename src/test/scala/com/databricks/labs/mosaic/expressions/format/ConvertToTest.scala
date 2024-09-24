package com.databricks.labs.mosaic.expressions.format

import com.databricks.labs.mosaic.core.geometry.api.JTS
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ConvertToTest extends QueryTest with SharedSparkSession with ConvertToBehaviors {

    private val noCodegen =
        withSQLConf(
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
          SQLConf.CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.NO_CODEGEN.toString
        ) _

    test("Testing convertTo checkInputTypeBehavior (H3, JTS)") { noCodegen { checkInputTypeBehavior(H3IndexSystem, JTS) } }
    test("Testing convertTo checkInputTypeBehavior (BNG, JTS)") { noCodegen { checkInputTypeBehavior(BNGIndexSystem, JTS) } }
    test("Testing convertTo passthroughBehavior (H3, JTS)") { noCodegen { passthroughBehavior(H3IndexSystem, JTS) } }
    test("Testing convertTo passthroughBehavior (BNG, JTS)") { noCodegen { passthroughBehavior(BNGIndexSystem, JTS) } }
    test("Testing stArea auxiliaryMethods (H3, JTS)") { noCodegen { auxiliaryMethods(H3IndexSystem, JTS) } }
    test("Testing stArea auxiliaryMethods (BNG, JTS)") { noCodegen { auxiliaryMethods(BNGIndexSystem, JTS) } }

}
