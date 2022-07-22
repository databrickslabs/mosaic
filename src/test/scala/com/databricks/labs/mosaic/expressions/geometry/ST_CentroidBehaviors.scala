package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator}
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.scalatest.matchers.must.Matchers.noException
import org.scalatest.matchers.should.Matchers.{an, be, convertToAnyShouldWrapper}

trait ST_CentroidBehaviors extends QueryTest {

    def centroidBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        val sc = spark
        import sc.implicits._
        mc.register(spark)

        val expected = mocks
            .getWKTRowsDf(mc)
            .orderBy("id")
            .select("wkt")
            .as[String]
            .collect()
            .map(wkt => mc.getGeometryAPI.geometry(wkt, "WKT").getCentroid)
            .map(c => (c.getX, c.getY))

        val result = mocks
            .getWKTRowsDf(mc)
            .select(st_centroid2D($"wkt").alias("coord"))
            .selectExpr("coord.*")
            .as[(Double, Double)]
            .collect()

        result.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

        mocks.getWKTRowsDf(mc).createOrReplaceTempView("source")

        val sqlResult = spark
            .sql("""with subquery (
                   | select st_centroid2D(wkt) as coord from source
                   |) select coord.* from subquery""".stripMargin)
            .as[(Double, Double)]
            .collect()

        sqlResult.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

    def centroidCodegen(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val result = mocks
            .getWKTRowsDf(mc)
            .select(st_centroid2D($"wkt").alias("coord"))
            .selectExpr("coord.*")

        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        val stCentroid = ST_Centroid(lit(1).expr, "JTS")
        val ctx = new CodegenContext
        an[Error] should be thrownBy stCentroid.genCode(ctx)
    }

    def auxiliaryMethods(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register(spark)

        val stCentroid = ST_Centroid(lit("POLYGON (1 1, 2 2, 3 3, 1 1)").expr, "illegalAPI")

        stCentroid.child shouldEqual lit("POLYGON (1 1, 2 2, 3 3, 1 1)").expr
        stCentroid.dataType shouldEqual StructType(Seq(StructField("x", DoubleType), StructField("y", DoubleType)))
        noException should be thrownBy stCentroid.makeCopy(Array(stCentroid.child))

    }

}
