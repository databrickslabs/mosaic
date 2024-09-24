package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.{mocks, MosaicSpatialQueryTest}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator}
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.scalatest.matchers.must.Matchers.noException
import org.scalatest.matchers.should.Matchers.{an, be, convertToAnyShouldWrapper}

trait ST_CentroidBehaviors extends MosaicSpatialQueryTest {

    def centroidBehavior(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
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
            .map(wkt => mc.getGeometryAPI.geometry(wkt, "WKT").getCentroid)
            .map(c => (c.getX, c.getY))

        val result = mocks
            .getWKTRowsDf()
            .select(st_centroid($"wkt").alias("centroid"))
            .select(st_x($"centroid").alias("x"), st_y($"centroid").alias("y"))
            .as[(Double, Double)]
            .collect()

        result.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

        mocks.getWKTRowsDf().createOrReplaceTempView("source")

        val sqlResult = spark
            .sql("""with subquery (
                   | select st_centroid(wkt) as coord from source
                   |) select st_x(coord), st_y(coord) from subquery""".stripMargin)
            .as[(Double, Double)]
            .collect()

        sqlResult.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

        val sqlResult2 = spark
            .sql(
                """with subquery (
                  | select st_centroid2D(wkt) as coord from source
                  |) select coord.col1, coord.col2 from subquery""".stripMargin)
            .as[(Double, Double)]
            .collect()

        sqlResult2.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

        noException should be thrownBy st_centroid2D(lit("POLYGON (1 1, 2 2, 3 3, 1 1)"))
    }

    def centroidCodegen(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val result = mocks
            .getWKTRowsDf()
            .select(st_centroid($"wkt").alias("centroid"))
            .select(st_x($"centroid").alias("x"), st_y($"centroid").alias("y"))

        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        val stCentroid = ST_Centroid(lit(1).expr, mc.expressionConfig)
        val ctx = new CodegenContext
        an[Error] should be thrownBy stCentroid.genCode(ctx)
    }

    def auxiliaryMethods(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        mc.register(spark)

        val stCentroid = ST_Centroid(lit("POLYGON (1 1, 2 2, 3 3, 1 1)").expr, mc.expressionConfig)

        stCentroid.child shouldEqual lit("POLYGON (1 1, 2 2, 3 3, 1 1)").expr
        stCentroid.dataType shouldEqual StringType
        noException should be thrownBy stCentroid.makeCopy(Array(stCentroid.child))
    }

}
