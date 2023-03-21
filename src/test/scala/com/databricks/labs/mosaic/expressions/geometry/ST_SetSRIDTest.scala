package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ST_SetSRIDTest extends QueryTest with SharedSparkSession with ST_SetSRIDBehaviors {

    private val noCodegen =
        withSQLConf(
            SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
            SQLConf.CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.NO_CODEGEN.toString
        ) _

    test("Testing stSetSRID (H3, JTS) NO_CODEGEN") { noCodegen { setSRIDBehaviour(H3IndexSystem, JTS) } }
    test("Testing stSetSRID (H3, ESRI) NO_CODEGEN") { noCodegen { setSRIDBehaviour(H3IndexSystem, ESRI) } }
    test("Testing stSetSRID (BNG, JTS) NO_CODEGEN") { noCodegen { setSRIDBehaviour(BNGIndexSystem, JTS) } }
    test("Testing stSetSRID (BNG, ESRI) NO_CODEGEN") { noCodegen { setSRIDBehaviour(BNGIndexSystem, ESRI) } }
    test("Testing stSetSRID auxiliaryMethods (H3, JTS)") { noCodegen { auxiliaryMethods(H3IndexSystem, JTS) } }
    test("Testing stSetSRID auxiliaryMethods (H3, ESRI)") { noCodegen { auxiliaryMethods(H3IndexSystem, ESRI) } }
    test("Testing stSetSRID auxiliaryMethods (BNG, JTS)") { noCodegen { auxiliaryMethods(BNGIndexSystem, JTS) } }
    test("Testing stSetSRID auxiliaryMethods (BNG, ESRI)") { noCodegen { auxiliaryMethods(BNGIndexSystem, ESRI) } }

}
