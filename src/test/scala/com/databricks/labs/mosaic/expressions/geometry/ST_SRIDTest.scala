package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ST_SRIDTest extends QueryTest with SharedSparkSession with ST_SRIDBehaviors {

    private val noCodegen =
        withSQLConf(
            SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
            SQLConf.CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.NO_CODEGEN.toString
        ) _

    private val codegenOnly =
        withSQLConf(
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
            SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
            SQLConf.CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.CODEGEN_ONLY.toString
        ) _

    test("Testing stSRID (H3, JTS) NO_CODEGEN") { noCodegen { SRIDBehaviour(H3IndexSystem, JTS) } }
    test("Testing stSRID (H3, ESRI) NO_CODEGEN") { noCodegen { SRIDBehaviour(H3IndexSystem, ESRI) } }
    test("Testing stSRID (BNG, JTS) NO_CODEGEN") { noCodegen { SRIDBehaviour(BNGIndexSystem, JTS) } }
    test("Testing stSRID (BNG, ESRI) NO_CODEGEN") { noCodegen { SRIDBehaviour(BNGIndexSystem, ESRI) } }
    test("Testing xMin (H3, JTS) CODEGEN compilation") { codegenOnly { SRIDCodegen(H3IndexSystem, JTS) } }
    test("Testing xMin (H3, ESRI) CODEGEN compilation") { codegenOnly { SRIDCodegen(H3IndexSystem, ESRI) } }
    test("Testing xMin (BNG, JTS) CODEGEN compilation") { codegenOnly { SRIDCodegen(BNGIndexSystem, JTS) } }
    test("Testing xMin (BNG, ESRI) CODEGEN compilation") { codegenOnly { SRIDCodegen(BNGIndexSystem, ESRI) } }
    test("Testing xMin (H3, JTS) CODEGEN_ONLY") { codegenOnly { SRIDBehaviour(H3IndexSystem, JTS) } }
    test("Testing xMin (H3, ESRI) CODEGEN_ONLY") { codegenOnly { SRIDBehaviour(H3IndexSystem, ESRI) } }
    test("Testing xMin (BNG, JTS) CODEGEN_ONLY") { codegenOnly { SRIDBehaviour(BNGIndexSystem, JTS) } }
    test("Testing xMin (BNG, ESRI) CODEGEN_ONLY") { codegenOnly { SRIDBehaviour(BNGIndexSystem, ESRI) } }
    test("Testing stSRID auxiliaryMethods (H3, JTS)") { noCodegen { auxiliaryMethods(H3IndexSystem, JTS) } }
    test("Testing stSRID auxiliaryMethods (H3, ESRI)") { noCodegen { auxiliaryMethods(H3IndexSystem, ESRI) } }
    test("Testing stSRID auxiliaryMethods (BNG, JTS)") { noCodegen { auxiliaryMethods(BNGIndexSystem, JTS) } }
    test("Testing stSRID auxiliaryMethods (BNG, ESRI)") { noCodegen { auxiliaryMethods(BNGIndexSystem, ESRI) } }

}
