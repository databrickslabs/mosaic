package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.{mocks, MosaicSpatialQueryTest}
import com.databricks.labs.mosaic.test.mocks.getWKTRowsDf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.QueryTest.checkAnswer
import org.scalatest.matchers.must.Matchers.{be, noException}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

trait ST_DimensionBehaviors extends MosaicSpatialQueryTest {

    def dimensionBehavior(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        import mc.functions._
        val sc = spark
        import sc.implicits._
        mc.register(spark)

        val df = mocks.getWKTRowsDf()

        val expected = df
            .orderBy("id")
            .select("wkt")
            .as[String]
            .collect()
            .map(wkt => mc.getGeometryAPI.geometry(wkt, "WKT").getDimension)
            .map(Row(_))

        val results = df.select(st_dimension($"wkt"))

        checkAnswer(results, expected)

        mocks.getWKTRowsDf().createOrReplaceTempView("source")

        val sqlResult = spark.sql("select st_dimension(wkt) from source")

        checkAnswer(sqlResult, expected)

        noException should be thrownBy df.select(try_sql(st_dimension($"wkt"))).limit(1).collect()
    }

    def areaCodegen(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        import mc.functions._
        val sc = spark
        import sc.implicits._
        mc.register(spark)

        val df = mocks.getWKTRowsDf()

        val result = df.select(st_dimension($"wkt"))

        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)
    }

    def auxiliaryMethods(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        mc.register(spark)

        val df = getWKTRowsDf()

        val stDimension = ST_Dimension(df.col("wkt").expr, mc.expressionConfig)

        stDimension.child shouldEqual df.col("wkt").expr
        stDimension.dataType shouldEqual DoubleType
        noException should be thrownBy stDimension.makeCopy(Array(stDimension.child))
        noException should be thrownBy ST_Dimension.unapply(stDimension)
        noException should be thrownBy ST_Dimension.apply(stDimension.child, mc.expressionConfig)
    }

}
