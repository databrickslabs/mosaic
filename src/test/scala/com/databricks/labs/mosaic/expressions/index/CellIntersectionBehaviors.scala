package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.types.ChipType
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.{MosaicSpatialQueryTest, mocks}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, lit, struct}
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers._

trait CellIntersectionBehaviors extends MosaicSpatialQueryTest {

    def behaviorComputedColumns(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        import mc.functions._
        mc.register(spark)
        val sc = spark
        import sc.implicits._

        val s = StructType(
          Array(
            StructField("case_id", IntegerType, true),
            StructField("is_core", BooleanType, true),
            StructField("index_id", LongType, true),
            StructField("wkt", StringType, true)
          )
        )

        val left = Seq(
          Row(1, true, 1L, "POLYGON ((0 0, 2 0, 2 1, 0 1, 0 0))"),
          Row(2, true, 1L, "POLYGON ((0 0, 2 0, 2 1, 0 1, 0 0))"),
          Row(3, false, 1L, "POLYGON ((0 0, 2 0, 2 1, 0 1, 0 0))")
        )
        val right = Seq(
          Row(1, true, 1L, "POLYGON ((1 0, 3 0, 3 1, 1 1, 1 0))"),
          Row(2, false, 1L, "POLYGON ((1 0, 3 0, 3 1, 1 1, 1 0))"),
          Row(3, false, 1L, "POLYGON ((1 0, 3 0, 3 1, 1 1, 1 0))")
        )

        val leftDf = spark
            .createDataFrame(spark.sparkContext.parallelize(left), s)
            .withColumn("wkb", st_aswkb($"wkt"))
            .select($"case_id", struct("is_core", "index_id", "wkb").alias("left_chip"))
        val rightDf = spark
            .createDataFrame(spark.sparkContext.parallelize(right), s)
            .withColumn("wkb", st_aswkb($"wkt"))
            .select($"case_id", struct("is_core", "index_id", "wkb").alias("right_chip"))

        val expected = Seq(
          Row(1, "POLYGON ((0 0, 2 0, 2 1, 0 1, 0 0))"),
          Row(2, "POLYGON ((1 0, 3 0, 3 1, 1 1, 1 0))"),
          Row(3, "POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))")
        )
        val expectedSchema = StructType(
          Array(
            StructField("case_id", IntegerType, true),
            StructField("expected_wkt", StringType, true)
          )
        )
        val expDf = spark.createDataFrame(spark.sparkContext.parallelize(expected), expectedSchema)

        val res = leftDf
            .join(rightDf, "case_id")
            .withColumn("intersection", grid_cell_intersection($"left_chip", $"right_chip"))
            .select($"case_id", st_aswkt($"intersection.wkb").alias("actual_wkt"))
            .join(expDf, "case_id")
            .select($"actual_wkt", $"expected_wkt")
            .as[(String, String)]
            .collect()
            .map(r => (mc.getGeometryAPI.geometry(r._1, "WKT"), mc.getGeometryAPI.geometry(r._2, "WKT")))

        res.foreach { case (actual, expected) => actual.equalsTopo(expected) shouldEqual true }


        leftDf.createOrReplaceTempView("left")

        val sqlResult = spark
            .sql("""with subquery (
                   | select grid_cell_intersection(left_chip, left_chip) as intersection from left
                   |) select st_aswkt(intersection.wkb) from subquery""".stripMargin)
            .as[String]
            .collect()
            .map(r => mc.getGeometryAPI.geometry(r, "WKT"))

        sqlResult.foreach(actual => actual.equalsTopo(mc.getGeometryAPI.geometry("POLYGON ((0 0, 2 0, 2 1, 0 1, 0 0))", "WKT")) shouldEqual true)
    }

    def columnFunctionSignatures(mosaicContext: MosaicContext): Unit = {
        val funcs = mosaicContext.functions
        noException should be thrownBy funcs.grid_cell_intersection(col("wkt"), col("wkt"))
    }

    def auxiliaryMethods(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val sc = spark
        import sc.implicits._
        val mc = mosaicContext
        mc.register(spark)

        val wkt = mocks.getWKTRowsDf(mc.getIndexSystem).limit(1).select("wkt").as[String].collect().head

        val cellIntersectionExpr = CellIntersection(
            lit(wkt).expr,
            lit(wkt).expr,
            mc.getIndexSystem,
            mc.getGeometryAPI.name
        )

        cellIntersectionExpr.dataType shouldEqual ChipType(LongType)

        val badExpr = CellIntersection(
            lit(10).expr,
            lit(true).expr,
            mc.getIndexSystem,
            mc.getGeometryAPI.name
        )

        noException should be thrownBy mc.functions.grid_cell_intersection(lit(""), lit(""))
        noException should be thrownBy cellIntersectionExpr.makeCopy(cellIntersectionExpr.children.toArray)
        noException should be thrownBy cellIntersectionExpr.withNewChildrenInternal(Array(cellIntersectionExpr.children: _*))
    }

}
