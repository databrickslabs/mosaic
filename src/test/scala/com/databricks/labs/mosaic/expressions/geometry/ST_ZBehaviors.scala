package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.{mocks, MosaicSpatialQueryTest}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator}
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.scalatest.matchers.must.Matchers.noException
import org.scalatest.matchers.should.Matchers.{an, be, convertToAnyShouldWrapper}

trait ST_ZBehaviors extends MosaicSpatialQueryTest {

    def stzBehavior(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = mosaicContext
        import mc.functions._
        val sc = spark
        import sc.implicits._
        mc.register(spark)

        val expected = mocks
            .getWKTRowsDf()
            .orderBy("id")
            .select("wkt")
            .as[String]
            .collect()
            .map(wkt => mc.getGeometryAPI.geometry(wkt, "WKT"))
            .map(c => c.getZ)

        val result = mocks
            .getWKTRowsDf()
            .select(st_z($"wkt").alias("z"))
            .as[Double]
            .collect()

        result.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

        mocks.getWKTRowsDf().createOrReplaceTempView("source")

        val sqlResult = spark
            .sql("""select st_z(wkt) from source""".stripMargin)
            .as[Double]
            .collect()

        sqlResult.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

    def stxCodegen(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val result = mocks
            .getWKTRowsDf()
            .select(st_z($"wkt").alias("z"))

        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        val stZ = ST_Z(lit(1).expr, mc.expressionConfig)
        val ctx = new CodegenContext
        an[Error] should be thrownBy stZ.genCode(ctx)
    }

    def auxiliaryMethods(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = mosaicContext
        mc.register(spark)

        val stZ = ST_Z(lit("POINT (2 3 4)").expr, mc.expressionConfig)

        stZ.child shouldEqual lit("POINT (2 3 4)").expr
        stZ.dataType shouldEqual DoubleType
        noException should be thrownBy stZ.makeCopy(Array(stZ.child))
    }

}
