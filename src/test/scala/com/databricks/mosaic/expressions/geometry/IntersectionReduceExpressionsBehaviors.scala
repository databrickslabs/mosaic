package com.databricks.mosaic.expressions.geometry

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.test.mocks._

trait IntersectionReduceExpressionsBehaviors { this: AnyFlatSpec =>

    def intersects(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs

        val left = boroughs
            .select(
              col("id").alias("left_id"),
              mosaic_explode(col("wkt"), 11).alias("left_index")
            )

        val right = boroughs
            .select(
              col("id"),
              st_translate(col("wkt"), sqrt(st_area(col("wkt")) * rand() * 0.1), sqrt(st_area(col("wkt")) * rand() * 0.1)).alias("wkt")
            )
            .select(
              col("id").alias("right_id"),
              mosaic_explode(col("wkt"), 11).alias("right_index")
            )

        val result = left
            .join(
              right,
              col("left_index.index_id") === col("right_index.index_id")
            )
            .groupBy(
              "left_id",
              "right_id"
            )
            .agg(
              st_reduce_intersects(col("left_index"), col("right_index"))
            )

        result.collect().length should be > 0

        left.createOrReplaceTempView("left")
        right.createOrReplaceTempView("right")

        val result2 = spark.sql("""
                                  |SELECT ST_REDUCE_INTERSECTS(LEFT_INDEX, RIGHT_INDEX)
                                  |FROM LEFT
                                  |INNER JOIN RIGHT ON LEFT_INDEX.INDEX_ID == RIGHT_INDEX.INDEX_ID
                                  |GROUP BY LEFT_ID, RIGHT_ID
                                  |""".stripMargin)

        result2.collect().length should be > 0

    }

    def intersection(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs

        val left = boroughs
            .select(
              col("wkt"),
              col("id").alias("left_id"),
              mosaic_explode(col("wkt"), 9).alias("left_index")
            )

        left.select(st_astext(col("left_index.wkb")).alias("tmp"))
            .withColumn("area", st_area(col("tmp")))
            .withColumn("cent", st_centroid2D(col("tmp")))
            .select("area", "cent", "tmp")
            .orderBy(desc("cent"))
            //.show(1000, truncate = false)

        val right = boroughs
            .select(
              col("id"),
              st_translate(col("wkt"), sqrt(st_area(col("wkt")) * 0.1), sqrt(st_area(col("wkt")) * 0.1)).alias("wkt")
            )
            .select(
              col("wkt"),
              col("id").alias("right_id"),
              mosaic_explode(col("wkt"), 9).alias("right_index")
            )

        val result = left
            .drop("wkt")
            .join(
              right,
              col("left_index.index_id") === col("right_index.index_id")
            )
            .groupBy(
              "left_id",
              "right_id"
            )
            .agg(
              st_reduce_intersection(col("left_index"), col("right_index")).alias("intersection")
            )
            .withColumn("area", st_area(col("intersection")))
            .withColumn("wkt", st_astext(col("intersection")))

        result
            .withColumn("area", st_area(col("intersection")))
            .withColumn("cent", st_centroid2D(col("intersection")))
            .select("cent", "area")
            .orderBy(desc("cent"))
            .show(truncate = false)
    }

}
