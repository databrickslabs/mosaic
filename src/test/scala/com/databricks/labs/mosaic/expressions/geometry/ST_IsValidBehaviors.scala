package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks.getWKTRowsDf
import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator}
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers.{all, an, be, convertToAnyShouldWrapper, noException}

//noinspection AccessorLikeMethodIsUnit
trait ST_IsValidBehaviors extends MosaicSpatialQueryTest {

    def isValidBehaviour(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val sc = spark
        val mc = mosaicContext
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val validDf = getWKTRowsDf().orderBy("id")
        val validResults = validDf.select(st_isvalid(col("wkt"))).as[Boolean].collect().toSeq

        all(validResults) should be(true)

        validDf.createOrReplaceTempView("source")
        val sqlValidResults = spark
            .sql("select st_isvalid(wkt) from source")
            .as[Boolean]
            .collect
            .toSeq

        all(sqlValidResults) should be(true)

        val invalidGeometries = List(
          List(
            "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (15 15, 15 20, 20 20, 20 15, 15 15))"
          ), // Hole Outside Shell
          List(
            "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2), (3 3, 3 7, 7 7, 7 3, 3 3))"
          ), // Nested Holes,
          List(
            "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (5 0, 10 5, 5 10, 0 5, 5 0))"
          ), // Disconnected Interior
          List("POLYGON((0 0, 10 10, 0 10, 10 0, 0 0))"), // Self Intersection
          List(
            "POLYGON((5 0, 10 0, 10 10, 0 10, 0 0, 5 0, 3 3, 5 6, 7 3, 5 0))"
          ), // Ring Self Intersection
          List(
            "MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)),(( 2 2, 8 2, 8 8, 2 8, 2 2)))"
          ), // Nested Shells
          List(
            "MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)),((0 0, 10 0, 10 10, 0 10, 0 0)))"
          ), // Duplicated Rings
          List("POLYGON((2 2, 8 2))"), // Too Few Points
          List("POLYGON((NaN 3, 3 4, 4 4, 4 3, 3 3))") // Invalid Coordinate
          // List("POLYGON((0 0, 0 10, 10 10, 10 0))") // Ring Not Closed
        )
        val rows = invalidGeometries.map { x => Row(x: _*) }
        val rdd = spark.sparkContext.makeRDD(rows)
        val schema = StructType(
          List(
            StructField("wkt", StringType)
          )
        )
        val invalidGeometriesDf = spark.createDataFrame(rdd, schema)

        val invalidDf = invalidGeometriesDf.withColumn("result", st_isvalid(col("wkt")))
        val invalidResults = invalidDf.select("result").as[Boolean].collect().toList

        all(invalidResults) should be(false)

        invalidDf.createOrReplaceTempView("source")
        val sqlInvalidResults = spark.sql("select st_isvalid(wkt) from source").collect.map(_.getBoolean(0)).toList

        all(sqlInvalidResults) should be(false)
    }

    def isValidCodegen(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val validDf = getWKTRowsDf().orderBy("id")
        val results = validDf.select(st_isvalid(col("wkt"))).as[Boolean]

        val queryExecution = results.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        val stIsValid = ST_IsValid(lit(1).expr, mc.expressionConfig)
        val ctx = new CodegenContext
        an[Error] should be thrownBy stIsValid.genCode(ctx)
    }

    def auxiliaryMethods(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        mc.register(spark)

        val stIsValid = ST_IsValid(lit("POLYGON (1 1, 2 2, 3 3, 4 4, 1 1)").expr, mc.expressionConfig)

        stIsValid.child shouldEqual lit("POLYGON (1 1, 2 2, 3 3, 4 4, 1 1)").expr
        stIsValid.dataType shouldEqual BooleanType
        noException should be thrownBy stIsValid.makeCopy(stIsValid.children.toArray)

    }

}
