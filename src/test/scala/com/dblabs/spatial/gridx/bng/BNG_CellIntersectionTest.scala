package com.dblabs.spatial.gridx.bng

import com.dblabs.spatial.vectorx.jts.JTS
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, StringType, StructField, StructType}

class BNG_CellIntersectionTest extends PlanTest with SharedSparkSession {

    test("BNG CellIntersection on sting ids") {
        import com.dblabs.spatial.gridx.bng.functions._
        import com.dblabs.spatial.udfs._
        com.dblabs.spatial.gridx.bng.functions.register(spark)

        spark.sparkContext.setLogLevel("ERROR")
        val sc = spark
        import sc.implicits._

        val s = StructType(
            Array(
                StructField("case_id", IntegerType, nullable = true),
                StructField("is_core", BooleanType, nullable = true),
                StructField("index_id", LongType, nullable = true),
                StructField("wkt", StringType, nullable = true)
            )
        )

        val left = Seq(
            Row(1, true, 1L, "POLYGON ((0 0, 2 0, 2 1, 0 1, 0 0))"),
            Row(2, false, 1L, "POLYGON ((0 0, 2 0, 2 1, 0 1, 0 0))"),
            Row(3, false, 1L, "POLYGON ((0 0, 2 0, 2 1, 0 1, 0 0))")
        )
        val right = Seq(
            Row(1, true, 1L, "POLYGON ((1 0, 3 0, 3 1, 1 1, 1 0))"),
            Row(2, true, 1L, "POLYGON ((1 0, 3 0, 3 1, 1 1, 1 0))"),
            Row(3, false, 1L, "POLYGON ((1 0, 3 0, 3 1, 1 1, 1 0))")
        )

        val leftDf = spark
            .createDataFrame(spark.sparkContext.parallelize(left), s)
            .withColumn("wkb", st_aswkb(col("wkt")))
            .select($"case_id", struct("is_core", "index_id", "wkb").alias("left_chip"))
        val rightDf = spark
            .createDataFrame(spark.sparkContext.parallelize(right), s)
            .withColumn("wkb", st_aswkb(col("wkt")))
            .select($"case_id", struct("is_core", "index_id", "wkb").alias("right_chip"))

        val expected = Seq(
            Row(1, "POLYGON ((0 0, 2 0, 2 1, 0 1, 0 0))"),
            Row(2, "POLYGON ((1 0, 3 0, 3 1, 1 1, 1 0))"),
            Row(3, "POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))")
        )
        val expectedSchema = StructType(
            Array(
                StructField("case_id", IntegerType, nullable = true),
                StructField("expected_wkt", StringType, nullable = true)
            )
        )
        val expDf = spark.createDataFrame(spark.sparkContext.parallelize(expected), expectedSchema)

        val res = leftDf
            .join(rightDf, "case_id")
            .repartition(3)
            .withColumn("intersection", bng_cell_intersection($"left_chip", $"right_chip"))
            .select($"case_id", st_aswkt($"intersection.wkb").alias("actual_wkt"))
            .join(expDf, "case_id")
            .select($"actual_wkt", $"expected_wkt")
            .as[(String, String)]
            .collect()


        res.foreach { case (actual, expected) =>
            assert(JTS.fromWKT(actual).equalsNorm(JTS.fromWKT(expected)))
            println(s"Actual: $actual, Expected: $expected")
        }



    }

}
