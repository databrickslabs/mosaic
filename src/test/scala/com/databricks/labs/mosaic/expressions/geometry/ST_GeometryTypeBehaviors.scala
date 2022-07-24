package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks.{getHexRowsDf, getWKTRowsDf}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator}
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StringType
import org.scalatest.matchers.must.Matchers.noException
import org.scalatest.matchers.should.Matchers.{an, be, convertToAnyShouldWrapper}

import java.util.Locale

trait ST_GeometryTypeBehaviors extends QueryTest {

    def wktTypeBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val df = getWKTRowsDf(mc)

        val results = df
            .select(st_geometrytype($"wkt").alias("result"))
            .as[String]
            .collect()
            .toList
            .sorted
            .map(_.toUpperCase(Locale.ROOT))
        val expected = List("LINESTRING", "MULTILINESTRING", "MULTIPOINT", "MULTIPOLYGON", "MULTIPOLYGON", "POINT", "POLYGON", "POLYGON")

        results.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

        df.createOrReplaceTempView("source")
        val sqlResults = spark
            .sql("select st_geometrytype(wkt) from source")
            .as[String]
            .collect
            .toList
            .sorted
            .map(_.toUpperCase(Locale.ROOT))

        sqlResults.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

    def wktTypesCodegen(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val df = getWKTRowsDf(mc)

        val result = df
            .crossJoin(df.withColumnRenamed("wkt", "other"))
            .select(st_geometrytype($"wkt").alias("result"))

        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        val stGeometryType = ST_GeometryType(lit(1).expr, "JTS")
        val ctx = new CodegenContext
        an[Error] should be thrownBy stGeometryType.genCode(ctx)
    }

    def hexTypesBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val df = getHexRowsDf(mc).select(as_hex($"hex").alias("hex"))

        val results = df
            .select(st_geometrytype($"hex").alias("result"))
            .orderBy("result")
            .as[String]
            .collect()
            .toList
            .sorted
            .map(_.toUpperCase(Locale.ROOT))

        val expected = List("LINESTRING", "MULTILINESTRING", "MULTIPOINT", "MULTIPOLYGON", "MULTIPOLYGON", "POINT", "POLYGON", "POLYGON")

        results.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

        df.createOrReplaceTempView("source")
        val sqlResults = spark
            .sql("select st_geometrytype(hex) from source")
            .as[String]
            .collect
            .toList
            .sorted
            .map(_.toUpperCase(Locale.ROOT))

        sqlResults.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

    def hexTypesCodegen(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val df = getHexRowsDf(mc)

        val result = df
            .crossJoin(df.withColumnRenamed("hex", "other"))
            .orderBy("other")
            .select(st_geometrytype($"hex").alias("result"))

        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        val stGeometryType = ST_GeometryType(lit("POINT (1 1)").expr, "illegalAPI")
        val ctx = new CodegenContext
        an[Error] should be thrownBy stGeometryType.genCode(ctx)

    }

    def auxiliaryMethods(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register(spark)

        val stGeometryType = ST_GeometryType(lit("POINT (1 1)").expr, geometryAPI.name)

        stGeometryType.child shouldEqual lit("POINT (1 1)").expr
        stGeometryType.dataType shouldEqual StringType
        noException should be thrownBy stGeometryType.makeCopy(Array(stGeometryType.child))

    }

}
