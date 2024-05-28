package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator}
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DoubleType
import org.scalatest.matchers.must.Matchers.noException
import org.scalatest.matchers.should.Matchers.{an, be, convertToAnyShouldWrapper}

import scala.collection.immutable

trait ST_DistanceBehaviors extends QueryTest {

    def distanceBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        val sc = spark
        import sc.implicits._
        mc.register(spark)

        val referenceGeoms = mocks
            .getWKTRowsDf()
            .orderBy("id")
            .select("wkt")
            .as[String]
            .collect()
            .map(wkt => mc.getGeometryAPI.geometry(wkt, "WKT"))

        val coords = referenceGeoms.head.getShells
        val pointsWKT = coords.map(_.toWKT)
        val pointWKTCompared = pointsWKT.zip(pointsWKT.tail)
        val expected = coords.zip(coords.tail).map({ case (a, b) => a.distance(b) })

        val df = pointWKTCompared.toDF("leftGeom", "rightGeom")

        val result = df.select(st_distance($"leftGeom", $"rightGeom")).as[Double].collect()

        result.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

        df.createOrReplaceTempView("source")
        val sqlResult = spark.sql("select st_distance(leftGeom, rightGeom) from source").as[Double].collect()

        sqlResult.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

    def distanceCodegen(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val referenceGeoms: immutable.Seq[MosaicGeometry] =
            mocks.wkt_rows_epsg4326.map(_(1).asInstanceOf[String]).map(mc.getGeometryAPI.geometry(_, "WKT"))

        val coords = referenceGeoms.head.getShells
        val df = coords.map(_.toWKT).toDF("point")

        val result = df
            .withColumnRenamed("point", "pointLeft")
            .crossJoin(df.withColumnRenamed("point", "pointRight"))
            .select(st_distance($"pointLeft", st_asbinary($"pointRight")))

        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        val stDistance = ST_Distance(lit(1).expr, lit("POINT (2 2)").expr, mc.expressionConfig)
        val ctx = new CodegenContext
        an[Error] should be thrownBy stDistance.genCode(ctx)
    }

    def auxiliaryMethods(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register(spark)

        val stDistance = ST_Distance(lit("POINT (1 1)").expr, lit("POINT (2 2) ").expr, mc.expressionConfig)

        stDistance.left shouldEqual lit("POINT (1 1)").expr
        stDistance.right shouldEqual lit("POINT (2 2) ").expr
        stDistance.dataType shouldEqual DoubleType
        noException should be thrownBy stDistance.makeCopy(Array(stDistance.left, stDistance.right))

    }

}
