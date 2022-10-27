package com.databricks.labs.mosaic.test

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.plans.PlanTest
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem, IndexSystem}
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.{Assertions, BeforeAndAfterEach, Suite, Tag}

abstract class MosaicSpatialQueryTest extends PlanTest with MosaicHelper {

    private val geometryApis = Seq(ESRI, JTS)

    private val indexSystems = Seq(H3IndexSystem, BNGIndexSystem)

    protected def spark: SparkSession

    protected def testAllGeometriesCodegen(testName: String, testTags: Tag*)(testFun: MosaicContext => Unit): Unit = {
        val is = MockIndexSystem
        for (geom <- geometryApis) {
            super.test(testName + s" (codegen) (${geom.name}, ${is.name})", testTags: _*)(
              withSQLConf(
                SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
                SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
                SQLConf.CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.CODEGEN_ONLY.toString
              ) {
                  withMosaicConf(geom, is) {
                      testFun
                  }
              }
            )

        }
    }

    protected def testAllGeometriesNoCodegen(testName: String, testTags: Tag*)(testFun: MosaicContext => Unit): Unit = {
        val is = MockIndexSystem
        for (geom <- geometryApis) {
            super.test(testName + s" (no codegen) (${geom.name}, ${is.name})", testTags: _*)(
              withSQLConf(
                SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
                SQLConf.CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.NO_CODEGEN.toString
              ) {
                  withMosaicConf(geom, is) {
                      testFun
                  }
              }
            )
        }
    }

    protected def testAllCodegen(testName: String, testTags: Tag*)(testFun: MosaicContext => Unit): Unit = {
        for (geom <- geometryApis) {
            for (is <- indexSystems) {
                super.test(testName + s" (codegen) (${geom.name}, ${is.name})", testTags: _*)(
                  withSQLConf(
                    SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
                    SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
                    SQLConf.CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.CODEGEN_ONLY.toString
                  ) {
                      withMosaicConf(geom, is) {
                          testFun
                      }
                  }
                )
            }
        }
    }

    protected def testAllNoCodegen(testName: String, testTags: Tag*)(testFun: MosaicContext => Unit): Unit = {
        for (geom <- geometryApis) {
            for (is <- indexSystems) {
                super.test(testName + s" (no codegen) (${geom.name}, ${is.name})", testTags: _*)(
                  withSQLConf(
                    SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
                    SQLConf.CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.NO_CODEGEN.toString
                  ) {
                      withMosaicConf(geom, is) {
                          testFun
                      }
                  }
                )
            }
        }
    }

    def checkGeometryTopo(
        mc: MosaicContext,
        actualAnswer: DataFrame,
        expectedAnswer: DataFrame,
        geometryFieldName: String
    ): Unit = {
        MosaicSpatialQueryTest.checkGeometryTopo(mc, actualAnswer, expectedAnswer, geometryFieldName)
    }

}

object MosaicSpatialQueryTest extends Assertions {
    def checkGeometryTopo(
        mc: MosaicContext,
        actualAnswer: DataFrame,
        expectedAnswer: DataFrame,
        geometryFieldName: String
    ): Unit = {
        import mc.functions.st_aswkt

        val actualGeoms = actualAnswer
            .withColumn("answer_wkt", st_aswkt(col(geometryFieldName)))
            .select(col("answer_wkt"))
            .collect()
            .map(_.getString(0))
            .map(mc.getGeometryAPI.geometry(_, "WKT"))
        val expectedGeoms = expectedAnswer
            .withColumn("answer_wkt", st_aswkt(col(geometryFieldName)))
            .select(col("answer_wkt"))
            .collect()
            .map(_.getString(0))
            .map(mc.getGeometryAPI.geometry(_, "WKT"))

        actualGeoms.zip(expectedGeoms).foreach { case (actualGeom, expectedGeom) =>
            assert(actualGeom.equalsTopo(expectedGeom), s"$actualGeom did not topologically equal $expectedGeom")
        }
    }
}

trait MosaicHelper extends BeforeAndAfterEach { self: Suite =>
    protected def withMosaicConf(geometry: GeometryAPI, indexSystem: IndexSystem)(f: MosaicContext => Unit): Unit = {
        val mc: MosaicContext = MosaicContext.build(indexSystem, geometry)
        f(mc)

    }
}
