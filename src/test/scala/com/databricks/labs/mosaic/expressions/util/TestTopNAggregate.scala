package com.databricks.labs.mosaic.expressions.util

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.ESRI
import com.databricks.labs.mosaic.core.index.H3IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.SparkSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec

class TestTopNAggregate extends AnyFlatSpec with SparkSuite {

    "Polyfill" should "fill wkt geometries for any index system and any geometry API" in {
        val mc = MosaicContext.build(H3IndexSystem, ESRI)
        import mc.functions._
        mc.register(spark)

        val rows = List(
          List(1, 10, 11, "abc"),
          List(1, 12, 11, "abc"),
          List(12, 10, 11, "abc"),
          List(1, 210, 11, "abc"),
          List(11, 10, 11, "abc"),
          List(1, 10, 12, "abc"),
          List(1, 12, 12, "abc"),
          List(12, 10, 12, "abc"),
          List(1, 210, 12, "abc"),
          List(11, 10, 12, "abc")
        ).map { x => Row(x: _*) }

        val rdd = spark.sparkContext.makeRDD(rows)
        val schema = StructType(
          List(
            StructField("id", IntegerType),
            StructField("price", IntegerType),
            StructField("count", IntegerType),
            StructField("name", StringType)
          )
        )

        val df = spark.createDataFrame(rdd, schema)

        df.groupBy("count")
            .agg(
              top_n_agg(struct(col("price"), col("*")), 2)
            )
            .show(truncate = false)
    }

}
