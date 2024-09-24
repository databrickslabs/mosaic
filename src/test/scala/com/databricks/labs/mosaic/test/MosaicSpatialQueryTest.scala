package com.databricks.labs.mosaic.test

import com.databricks.labs.mosaic.core.geometry.api.{GeometryAPI, JTS}
import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest._

/**
  * Provides helper methods for running tests against a matrix of geometry apis,
  * grid index systems and SQL confs.
  */
abstract class MosaicSpatialQueryTest extends PlanTest with MosaicHelper {

    private val geometryApis = Seq(JTS)

    private val indexSystems =
        Seq(
          H3IndexSystem,
          BNGIndexSystem,
          CustomIndexSystem(GridConf(-180, 180, -90, 90, 2, 360, 180))
        )

    def checkGeometryTopo(
        mc: MosaicContext,
        actualAnswer: DataFrame,
        expectedAnswer: DataFrame,
        geometryFieldName: String
    ): Unit = {
        MosaicSpatialQueryTest.checkGeometryTopo(mc, actualAnswer, expectedAnswer, geometryFieldName)
    }

    protected def spark: SparkSession

    /**
      * Runs the testcase with all different geometry APIs while the grid index
      * system is mocked out. Tests the codegen path of the query plan.
      */
    protected def testAllGeometriesCodegen(testName: String, testTags: Tag*)(testFun: MosaicContext => Unit): Unit = {
        val is = MockIndexSystem
        for (geom <- geometryApis) {
            super.test(testName + s" (codegen) (${geom.name}, ${is.name})", testTags: _*)(
              withSQLConf(
                SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
                SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
                SQLConf.CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.CODEGEN_ONLY.toString,
                "spark.sql.parquet.compression.codec" -> "uncompressed"
              ) {
                  spark.sparkContext.setLogLevel("ERROR")
                  withMosaicContext(geom, is) {
                      testFun
                  }
              }
            )

        }
    }

    /**
      * Runs the testcase with all different geometry APIs while the grid index
      * system is mocked out. Tests the interpreted path of the query plan.
      */
    protected def testAllGeometriesNoCodegen(testName: String, testTags: Tag*)(testFun: MosaicContext => Unit): Unit = {
        val is = MockIndexSystem
        for (geom <- geometryApis) {
            super.test(testName + s" (no codegen) (${geom.name}, ${is.name})", testTags: _*)(
              withSQLConf(
                SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
                SQLConf.CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.NO_CODEGEN.toString,
                "spark.sql.parquet.compression.codec" -> "uncompressed"
              ) {
                  spark.sparkContext.setLogLevel("ERROR")
                  withMosaicContext(geom, is) {
                      testFun
                  }
              }
            )
        }
    }

    /**
      * Runs the testcase with all valid combinations of geometry API + grid
      * index system. Tests the codegen path of the query plan.
      */
    protected def testAllCodegen(testName: String, testTags: Tag*)(testFun: MosaicContext => Unit): Unit = {
        for (geom <- geometryApis) {
            for (is <- indexSystems) {
                super.test(testName + s" (codegen) (${geom.name}, ${is.name})", testTags: _*)(
                  withSQLConf(
                    SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
                    SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
                    SQLConf.CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.CODEGEN_ONLY.toString,
                    "spark.sql.parquet.compression.codec" -> "uncompressed"
                  ) {
                      spark.sparkContext.setLogLevel("ERROR")
                      withMosaicContext(geom, is) {
                          testFun
                      }
                  }
                )
            }
        }
    }

    /**
      * Runs the testcase with all valid combinations of geometry API + grid
      * index system. Tests the interpreted path of the query plan.
      */
    protected def testAllNoCodegen(testName: String, testTags: Tag*)(testFun: MosaicContext => Unit): Unit = {
        for (geom <- geometryApis) {
            for (is <- indexSystems) {
                super.test(testName + s" (no codegen) (${geom.name}, ${is.name})", testTags: _*)(
                  withSQLConf(
                    SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
                    SQLConf.CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.NO_CODEGEN.toString,
                    "spark.sql.parquet.compression.codec" -> "uncompressed"
                  ) {
                      spark.sparkContext.setLogLevel("ERROR")
                      withMosaicContext(geom, is) {
                          testFun
                      }
                  }
                )
            }
        }
    }

}

object MosaicSpatialQueryTest extends Assertions {

    /**
      * Runs the query plan and checks if the answer is toplogogically equal to
      * the expected result.
      *
      * @param mc
      *   the mosaic context that performs the equality check.
      * @param actualAnswer
      *   the actual result as a [[DataFrame]].
      * @param expectedAnswer
      *   the expected result as a [[DataFrame]].
      * @param geometryFieldName
      *   the name of the column containing the geometries to be compared.
      */
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
            .orderBy("answer_wkt")
            .collect()
            .map(_.getString(0))
            .map(mc.getGeometryAPI.geometry(_, "WKT"))
        val expectedGeoms = expectedAnswer
            .withColumn("answer_wkt", st_aswkt(col(geometryFieldName)))
            .select(col("answer_wkt"))
            .orderBy("answer_wkt")
            .collect()
            .map(_.getString(0))
            .map(mc.getGeometryAPI.geometry(_, "WKT"))

        actualGeoms.zip(expectedGeoms).foreach { case (actualGeom, expectedGeom) =>
            assert(actualGeom.equalsTopo(expectedGeom), s"$actualGeom did not topologically equal $expectedGeom")
        }
    }
}

trait MosaicHelper extends BeforeAndAfterEach { self: Suite =>

    /** Constructs the MosaicContext from its parts and calls `f`. */
    protected def withMosaicContext(geometry: GeometryAPI, indexSystem: IndexSystem)(f: MosaicContext => Unit): Unit = {
        val mc: MosaicContext = MosaicContext.build(indexSystem, geometry)
        f(mc)

    }
}
