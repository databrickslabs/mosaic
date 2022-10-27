package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext}
import org.apache.spark.sql.catalyst.plans.CodegenInterpretedPlanTest
import org.apache.spark.sql.execution.WholeStageCodegenExec
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem, IndexSystem}
import org.apache.spark.sql.functions.{col, lit}
import org.scalatest.Tag
import org.scalatest.matchers.must.Matchers.noException
import org.scalatest.matchers.should.Matchers.{an, be, convertToAnyShouldWrapper}

trait MosaicHelper {
    protected def withMosaicConf(geometry: GeometryAPI, indexSystem: IndexSystem)(f: MosaicContext => Unit): Unit = {
        val mc: MosaicContext = MosaicContext.build(indexSystem, geometry)
        f(mc)
    }
}

//abstract class MosaicSpatialQueryTest extends CodegenInterpretedPlanTest with MosaicHelper {
//
//    private val geometryApis = Seq(JTS, ESRI)
//    private val indexSystems = Seq(H3IndexSystem, BNGIndexSystem)
//
//    protected def spark: SparkSession
//
//    protected def testAllGeometriesAllIndexSystems(testName: String, testTags: Tag*)(testFun: MosaicContext => Unit): Unit = {
//        for (geom <- geometryApis) {
//            for (is <- indexSystems) {
//                super.test(testName + s" (${geom.name}, ${is.name})", testTags: _*)(
//                  withMosaicConf(geom, is) { testFun }
//                )
//            }
//        }
//    }
//
//    protected def checkGeometryTopo(
//        mc: MosaicContext,
//        actualAnswer: DataFrame,
//        expectedAnswer: DataFrame,
//        geometryFieldName: String
//    ): Unit = {
//        import mc.functions.st_aswkt
//
//        val actualGeoms = actualAnswer
//            .withColumn("answer_wkt", st_aswkt(col(geometryFieldName)))
//            .select("answer_wkt")
//            .collect()
//            .map(mc.getGeometryAPI.geometry(_, "WKT"))
//        val expectedGeoms = expectedAnswer
//            .withColumn("answer_wkt", st_aswkt(col(geometryFieldName)))
//            .select("answer_wkt")
//            .collect()
//            .map(mc.getGeometryAPI.geometry(_, "WKT"))
//
//        actualGeoms.zip(expectedGeoms).foreach { case (actualGeom, expectedGeom) =>
//            assert(actualGeom.equals(expectedGeom), s"$actualGeom did not topologically equal $expectedGeom")
//        }
//    }
//
//}

trait ST_UnaryUnionBehaviours extends MosaicSpatialQueryTest {

    def uub(mc: MosaicContext): Unit = {
        val sc = spark
        import sc.implicits._
        import mc.functions._
        mc.register(sc)

        val multiPolygon = List("MULTIPOLYGON (((10 10, 20 10, 20 20, 10 20, 10 10)), ((15 15, 25 15, 25 25, 15 25, 15 15)))")
        val expected = List("POLYGON ((20 15, 20 10, 10 10, 10 20, 15 20, 15 25, 25 25, 25 15, 20 15))")
            .map(mc.getGeometryAPI.geometry(_, "WKT"))

        val results = multiPolygon
            .toDF("multiPolygon")
            .withColumn("result", st_unaryunion($"multiPolygon"))
            .select($"result")
            .as[String]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "WKT"))

        results.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

    }

    def unaryUnionBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        val sc = spark
        import sc.implicits._
        mc.register(spark)

        val multiPolygon = List("MULTIPOLYGON (((10 10, 20 10, 20 20, 10 20, 10 10)), ((15 15, 25 15, 25 25, 15 25, 15 15)))")
        val expected = List("POLYGON ((20 15, 20 10, 10 10, 10 20, 15 20, 15 25, 25 25, 25 15, 20 15))")
            .map(mc.getGeometryAPI.geometry(_, "WKT"))

        val results = multiPolygon
            .toDF("multiPolygon")
            .withColumn("result", st_unaryunion($"multiPolygon"))
            .select($"result")
            .as[String]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "WKT"))

        results.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

    def unaryUnionCodegen(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val result = mocks.getWKTRowsDf(mc).select(st_unaryunion($"wkt"))

        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        val stUnaryUnion = ST_UnaryUnion(lit(1).expr, "JTS")
        val ctx = new CodegenContext
        an[Error] should be thrownBy stUnaryUnion.genCode(ctx)
    }

    def auxiliaryMethods(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register(spark)

        val stUnaryUnion = ST_UnaryUnion(
          lit("MULTIPOLYGON (((10 10, 20 10, 20 20, 10 20, 10 10)), ((15 15, 25 15, 25 25, 15 25, 15 15)))").expr,
          "illegalAPI"
        )

        stUnaryUnion.child shouldEqual lit(
          "MULTIPOLYGON (((10 10, 20 10, 20 20, 10 20, 10 10)), ((15 15, 25 15, 25 25, 15 25, 15 15)))"
        ).expr
        stUnaryUnion.dataType shouldEqual lit(
          "MULTIPOLYGON (((10 10, 20 10, 20 20, 10 20, 10 10)), ((15 15, 25 15, 25 25, 15 25, 15 15)))"
        ).expr.dataType
        noException should be thrownBy stUnaryUnion.makeCopy(Array(stUnaryUnion.child))
    }

}
