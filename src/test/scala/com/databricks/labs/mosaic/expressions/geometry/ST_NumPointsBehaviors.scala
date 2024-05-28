package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.{mocks, MosaicSpatialQueryTest}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator}
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.scalatest.matchers.must.Matchers.noException
import org.scalatest.matchers.should.Matchers.{an, be, contain, convertToAnyShouldWrapper}

trait ST_NumPointsBehaviors extends MosaicSpatialQueryTest {

    def numPointsBehaviour(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val poly = """POLYGON ((10 10, 110 10, 110 110, 10 110, 10 10),
                     | (20 20, 20 30, 30 30, 30 20, 20 20),
                     | (40 20, 40 30, 50 30, 50 20, 40 20))""".stripMargin.filter(_ >= ' ')

        val line = """LINESTRING (10 10, 10 20, 20 11, 11 30)"""

        val rows = List(
          poly,
          line
        )

        val results = rows
            .toDF("geometry")
            .withColumn("num_points", st_numpoints($"geometry"))
            .select("num_points")
            .as[Int]
            .collect()

        results should contain theSameElementsAs Seq(15, 4)
    }

    def codegenCompilation(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        val sc = spark
        mc.register(sc)
        import mc.functions._
        import sc.implicits._

        val result = mocks.getWKTRowsDf().select(st_numpoints($"wkt"))

        val plan = result.queryExecution.executedPlan
        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])
        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()
        noException should be thrownBy CodeGenerator.compile(code)

        val stEnvelope = ST_NumPoints(lit(1).expr, mc.expressionConfig)
        val ctx = new CodegenContext
        an[Error] should be thrownBy stEnvelope.genCode(ctx)
    }

    def auxiliaryMethods(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        mc.register(spark)

        val stNumPoints = ST_NumPoints(lit("POLYGON (1 1, 2 2, 3 3, 4 4, 1 1)").expr, mc.expressionConfig)

        stNumPoints.child shouldEqual lit("POLYGON (1 1, 2 2, 3 3, 4 4, 1 1)").expr
        stNumPoints.dataType shouldEqual IntegerType
        noException should be thrownBy stNumPoints.makeCopy(stNumPoints.children.toArray)

    }

}
