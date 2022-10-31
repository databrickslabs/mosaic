package com.databricks.labs.mosaic.models

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

import scala.util.Try

class ApproximateSpatialKNNTest extends QueryTest with SharedSparkSession with ApproximateSpatialKNNBehaviors {

    //Hotfix for SharedSparkSession afterAll cleanup.
    override def afterAll(): Unit = Try(super.afterAll())

    private val noCodegen =
        withSQLConf(
            SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
            SQLConf.CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.NO_CODEGEN.toString
        ) _

    test("Test approximate knn for (H3, JTS).") { noCodegen { wktKNN(H3IndexSystem, JTS, 4, 100.0) } }
    test("Test approximate knn for (H3, ESRI).") { noCodegen { wktKNN(H3IndexSystem, ESRI, 4, 100.0) } }
    test("Test approximate knn for (BNG, JTS).") { noCodegen { wktKNN(BNGIndexSystem, JTS, -4, 10000.0) } }
    test("Test approximate knn for (BNG, ESRI).") { noCodegen { wktKNN(BNGIndexSystem, ESRI, -4, 10000.0) } }

}
