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

    test("Testing stSRID (H3, JTS) NO_CODEGEN") { noCodegen { SRIDBehaviour(H3IndexSystem, JTS) } }
    test("Testing stSRID (H3, ESRI) NO_CODEGEN") { noCodegen { SRIDBehaviour(H3IndexSystem, ESRI) } }
    test("Testing stSRID (BNG, JTS) NO_CODEGEN") { noCodegen { SRIDBehaviour(BNGIndexSystem, JTS) } }
    test("Testing stSRID (BNG, ESRI) NO_CODEGEN") { noCodegen { SRIDBehaviour(BNGIndexSystem, ESRI) } }
    test("Testing stSRID auxiliaryMethods (H3, JTS)") { noCodegen { auxiliaryMethods(H3IndexSystem, JTS) } }
    test("Testing stSRID auxiliaryMethods (H3, ESRI)") { noCodegen { auxiliaryMethods(H3IndexSystem, ESRI) } }
    test("Testing stSRID auxiliaryMethods (BNG, JTS)") { noCodegen { auxiliaryMethods(BNGIndexSystem, JTS) } }
    test("Testing stSRID auxiliaryMethods (BNG, ESRI)") { noCodegen { auxiliaryMethods(BNGIndexSystem, ESRI) } }

}
