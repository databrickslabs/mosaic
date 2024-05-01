package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{QueryTest, Row}
import org.scalatest.matchers.must.Matchers.{be, noException}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

trait ST_HaversineBehaviors extends QueryTest {

    def haversineBehaviour(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        mc.register(spark)

        val expected = Seq(Row(10007.55722101796))
        val result = spark.range(1).select(st_haversine(lit(0.0), lit(90.0), lit(0.0), lit(180.0)))

        checkAnswer(result, expected)
    }

    def haversineCodegen(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        mc.register(spark)

        val result = spark.range(1).select(st_haversine(lit(0.0), lit(90.0), lit(0.0), lit(0.0)))

        // Check if code generation was planned
        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan
        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])
        wholeStageCodegenExec.isDefined shouldBe true

        // Check is generated code compiles
        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()
        noException should be thrownBy CodeGenerator.compile(code)
    }

    def auxiliaryMethods(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register(spark)

        val stHaversine = ST_Haversine(
          lit(0.0).expr,
          lit(1.0).expr,
          lit(2.0).expr,
          lit(3.0).expr
        )

        stHaversine.first shouldEqual lit(0.0).expr
        stHaversine.second shouldEqual lit(1.0).expr
        stHaversine.third shouldEqual lit(2.0).expr
        stHaversine.fourth shouldEqual lit(3.0).expr
        stHaversine.dataType shouldEqual lit(3.141592).expr.dataType
        noException should be thrownBy stHaversine.makeCopy(
          Array(stHaversine.first, stHaversine.second, stHaversine.third, stHaversine.fourth)
        )
    }

}
