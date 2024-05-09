package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.{mocks, MosaicSpatialQueryTest}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._
import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.scalatest.matchers.must.Matchers.noException
import org.scalatest.matchers.should.Matchers.{an, be, convertToAnyShouldWrapper}

import scala.collection.JavaConverters._

trait ST_HasValidCoordinatesBehaviors extends MosaicSpatialQueryTest {

    // noinspection AccessorLikeMethodIsUnit
    def hasValidCoordinatesBehaviours(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("FATAL") // <- otherwise asserted exceptions print
        val mc = mosaicContext
        import mc.functions._
        mc.register(spark)

        val inputDf = testData(spark)

        val sourceDf = inputDf
            .select(col("toWKTRef").alias("wkt"))
            .withColumn("has_valid_coords", st_hasvalidcoordinates(col("wkt"), lit("EPSG:3857"), lit("reprojected_bounds")))

        sourceDf.count() shouldEqual sourceDf.where("has_valid_coords").count()

        inputDf.select(col("toWKTRef").alias("wkt")).createOrReplaceTempView("input")

        val sqlResult = spark
            .sql(s"""
                    |select
                    |    st_hasvalidcoordinates(wkt, 'EPSG:3857', 'reprojected_bounds') as has_valid_coords
                    |from input
                    |""".stripMargin)

        sqlResult.count() shouldEqual sqlResult.where("has_valid_coords").count()

        an[SparkException] should be thrownBy {
            inputDf
                .select(col("toWKTRef").alias("wkt"))
                .withColumn("has_valid_coords", st_hasvalidcoordinates(col("wkt"), lit("EPSG:3857"), lit("invalid_value")))
                .collect()
        }

        an[SparkException] should be thrownBy {
            inputDf
                .select(col("toWKTRef").alias("wkt"))
                .withColumn("has_valid_coords", st_hasvalidcoordinates(col("wkt"), lit("EPSG:invalid_value"), lit("bounds")))
                .collect()
        }

        val sourceDf2 = inputDf
            .select(col("fromWKT").alias("wkt"))
            .withColumn("has_valid_coords", st_hasvalidcoordinates(col("wkt"), lit("EPSG:4326"), lit("bounds")))

        sourceDf2.count() shouldEqual sourceDf2.where("has_valid_coords").count()

    }

    def testData(spark: SparkSession): DataFrame = {
        // Comparison vs. PostGIS
        val testDataWKT = List(
          (
            "POLYGON((30 10,40 40,20 40,10 20,30 10))",
            "POLYGON((3339584.723798207 1118889.9748579594,4452779.631730943 4865942.279503175,2226389.8158654715 4865942.279503175,1113194.9079327357 2273030.926987689,3339584.723798207 1118889.9748579594))"
          ),
          (
            "MULTIPOLYGON(((0 0,0 1,2 2,0 0)))",
            "MULTIPOLYGON(((0 0,0 111325.14286638508,222638.98158654713 222684.20850554403,0 0)))"
          ),
          (
            "MULTIPOLYGON(((40 60,20 45,45 30,40 60)), ((20 35,10 30,10 10,30 5,45 20,20 35), (30 20,20 15,20 25,30 20)))",
            "MULTIPOLYGON(((4452779.631730943 8399737.889818357,2226389.8158654715 5621521.486192066,5009377.085697311 3503549.8435043744,4452779.631730943 8399737.889818357)), ((2226389.8158654715 4163881.144064293,1113194.9079327357 3503549.8435043744,1113194.9079327357 1118889.9748579594,3339584.723798207 557305.2572745767,5009377.085697311 2273030.926987689,2226389.8158654715 4163881.144064293), (3339584.723798207 2273030.926987689,2226389.8158654715 1689200.1396078935,2226389.8158654715 2875744.6243522433,3339584.723798207 2273030.926987689)))"
          ),
          ("POINT(-75.78033 35.18937)", "POINT(-8435827.747746235 4189645.642183593)"),
          (
            "MULTIPOINT(10 40,40 30,20 20,30 10)",
            "MULTIPOINT(1113194.9079327357 4865942.279503175,4452779.631730943 3503549.8435043744,2226389.8158654715 2273030.926987689,3339584.723798207 1118889.9748579594)"
          ),
          (
            "LINESTRING(30 10,10 30,40 40)",
            "LINESTRING(3339584.723798207 1118889.9748579594,1113194.9079327357 3503549.8435043744,4452779.631730943 4865942.279503175)"
          )
        ).map({ case (f: String, t: String) => Row(f, t) })
        val testSchema = StructType(
          Seq(
            StructField("fromWKT", StringType),
            StructField("toWKTRef", StringType)
          )
        )

        val sourceDf = spark
            .createDataFrame(testDataWKT.asJava, testSchema)
        sourceDf
    }

    def expressionCodegen(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val result = mocks
            .getWKTRowsDf()
            .select(st_hasvalidcoordinates($"wkt", lit("EPSG:4326"), lit("bounds")))
            .as[String]

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

        val stHasValidCoords = ST_HasValidCoordinates(lit("POINT (1 1)").expr, lit("EPSG:4326").expr, lit("bounds").expr, mc.expressionConfig)

        stHasValidCoords.first shouldEqual lit("POINT (1 1)").expr
        stHasValidCoords.second shouldEqual lit("EPSG:4326").expr
        stHasValidCoords.third shouldEqual lit("bounds").expr

        stHasValidCoords.dataType shouldEqual BooleanType
        noException should be thrownBy stHasValidCoords.makeCopy(stHasValidCoords.children.toArray)

    }

}
