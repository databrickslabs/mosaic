package com.databricks.labs.mosaic.models.knn

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

import scala.util.Try

class HexRingNeighboursTest extends QueryTest with SharedSparkSession with HexRingNeighboursBehaviors {

    //Hotfix for SharedSparkSession afterAll cleanup.
    override def afterAll(): Unit = Try(super.afterAll())

    private val noCodegen =
        withSQLConf(
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
          SQLConf.CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.NO_CODEGEN.toString
        ) _

    test("Test leftTransform for HexRingNeighbours (H3, JTS).") { noCodegen { leftTransform(H3IndexSystem, JTS, 7) } }
    test("Test leftTransform for HexRingNeighbours (H3, ESRI).") { noCodegen { leftTransform(H3IndexSystem, ESRI, 7) } }
    test("Test leftTransform for HexRingNeighbours (BNG, JTS).") { noCodegen { leftTransform(BNGIndexSystem, JTS, 2) } }
    test("Test leftTransform for HexRingNeighbours (BNG, ESRI).") { noCodegen { leftTransform(BNGIndexSystem, ESRI, 2) } }
    test("Test resultTransform for HexRingNeighbours (H3, JTS).") { noCodegen { resultTransform(H3IndexSystem, JTS, 5) } }
    test("Test resultTransform for HexRingNeighbours (H3, ESRI).") { noCodegen { resultTransform(H3IndexSystem, ESRI, 5) } }
    test("Test resultTransform for HexRingNeighbours (BNG, JTS).") { noCodegen { resultTransform(BNGIndexSystem, JTS, -4) } }
    test("Test resultTransform for HexRingNeighbours (BNG, ESRI).") { noCodegen { resultTransform(BNGIndexSystem, ESRI, -4) } }
    test("Test transform for HexRingNeighbours (H3, JTS).") { noCodegen { transform(H3IndexSystem, JTS, 5, 3) } }
    test("Test transform for HexRingNeighbours (H3, ESRI).") { noCodegen { transform(H3IndexSystem, ESRI, 5, 3) } }
    test("Test transform for HexRingNeighbours (BNG, JTS).") { noCodegen { transform(BNGIndexSystem, JTS, 4, 15) } }
    test("Test transform for HexRingNeighbours (BNG, ESRI).") { noCodegen { transform(BNGIndexSystem, ESRI, -4, 1) } }

}
