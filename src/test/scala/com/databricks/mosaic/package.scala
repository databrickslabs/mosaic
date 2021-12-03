package com.databricks.mosaic

import org.apache.spark.sql.types.StringType

package object expressions {
  object mocks {

    import org.apache.spark.sql.types.{StructField, StructType}
    import org.apache.spark.sql.{DataFrame, Row, SparkSession}

    val hex_rows = List(
      List("00000000030000000100000005403E0000000000004024000000000000404400000000000040440000000000004034000000000000404400000000000040240000000000004034000000000000403E0000000000004024000000000000"),
      List("0106000020620D00000100000001030000000100000004000000000000000000000000000000000000000000000000000000000000000000F03F0000000000000040000000000000004000000000000000000000000000000000")
    )

    val wkt_rows = List(
      List("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"),
      List("MULTIPOLYGON (((0 0, 0 1, 2 2, 0 0)))")
    )

    def getHexRowsDf: DataFrame = {
      val spark = SparkSession.builder().getOrCreate()
      val rows = hex_rows.map { x => Row(x: _*) }
      val rdd = spark.sparkContext.makeRDD(rows)
      val schema = StructType(
        List(
          StructField("hex", StringType)
        )
      )
      val df = spark.createDataFrame(rdd, schema)
      df
    }

    def getWKTRowsDf: DataFrame = {
      val spark = SparkSession.builder().getOrCreate()
      val rows = wkt_rows.map { x => Row(x: _*) }
      val rdd = spark.sparkContext.makeRDD(rows)
      val schema = StructType(
        List(
          StructField("wkt", StringType)
        )
      )
      val df = spark.createDataFrame(rdd, schema)
      df
    }
  }
}
