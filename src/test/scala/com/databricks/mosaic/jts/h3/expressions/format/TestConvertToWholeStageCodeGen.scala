package com.databricks.mosaic.jts.h3.expressions.format

import scala.util.Try

import com.stephenn.scalatest.jsonassert.JsonMatchers
import org.scalatest.Matchers

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.WholeStageCodegenExec

import com.databricks.mosaic.core.geometry.api.GeometryAPI.OGC
import com.databricks.mosaic.core.index.H3IndexSystem
import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.mocks.getHexRowsDf
import com.databricks.mosaic.test.SparkFunSuite

case class TestConvertToWholeStageCodeGen() extends SparkFunSuite with Matchers with JsonMatchers {

    val mosaicContext: MosaicContext = MosaicContext.build(H3IndexSystem, OGC)

    import mosaicContext.functions._
    import testImplicits._

    test("WholeStageCodegen for WKB to WKT conversion.") {
        mosaicContext.register(spark)

        val hexDf: DataFrame = getHexRowsDf
            .withColumn("hex", as_hex($"hex"))
            .withColumn("wkb", convert_to($"hex", "WKB"))

        val left = hexDf
            .orderBy("id")
            .select(
              convert_to($"wkb", "WKT").alias("wkt")
            )

        val plan = left.queryExecution.executedPlan

        plan.foreach {
            case stage: WholeStageCodegenExec => Try(stage.doCodeGen()).isSuccess shouldBe true
            case _                            => true shouldBe true
        }

        hexDf.createOrReplaceTempView("format_testing_left")

        val left2 = spark.sql(
          "select convert_to_wkt(wkb) as wkt from format_testing_left order by id"
        )

        val left3 = spark.sql(
          "select st_aswkt(wkb) as wkt from format_testing_left order by id"
        )

        val left4 = spark.sql(
          "select st_astext(wkb) as wkt from format_testing_left order by id"
        )

    }

}
