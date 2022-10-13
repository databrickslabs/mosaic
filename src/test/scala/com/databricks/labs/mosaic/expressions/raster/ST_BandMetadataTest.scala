package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.JTS
import com.databricks.labs.mosaic.core.index.H3IndexSystem
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSessionGDAL

import scala.util.Try

class ST_BandMetadataTest extends QueryTest with SharedSparkSessionGDAL with ST_BandMetadataBehaviors {

    //Hotfix for SharedSparkSession afterAll cleanup.
    override def afterAll(): Unit = Try(super.afterAll())

    private val noCodegen =
        withSQLConf(
            SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
            SQLConf.CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.NO_CODEGEN.toString,
        ) _


    // These tests are not index system nor geometry API specific.
    // Only testing one pairing is sufficient.
    if (System.getProperty("os.name") == "Linux") {
        test("Testing ST_BandMetadataTest with manual GDAL registration (H3, JTS).") { noCodegen { bandMetadataBehavior(H3IndexSystem, JTS) } }
    } else {
        logWarning("Skipping ST_BandMetadataTest test on non-Linux OS")
    }

}
