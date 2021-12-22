package com.databricks.mosaic.expressions.format

import com.databricks.mosaic.expressions.format.Conversions.typed
import com.databricks.mosaic.functions.{as_hex, as_json, convert_to, register}
import com.databricks.mosaic.mocks.{getHexRowsDf, getWKTRowsDf, getGeoJSONDf}
import com.databricks.mosaic.test.SparkTest
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, from_json, schema_of_json}
import org.scalatest.{FunSuite, Matchers}
import com.stephenn.scalatest.jsonassert.JsonMatchers

class TestConvertTo extends FunSuite with SparkTest with Matchers with JsonMatchers {

  ignore("Conversion from WKB to WKT") {

    val hexDf: DataFrame = getHexRowsDf
      .withColumn("wkb", convert_to(as_hex(col("hex")), "WKB"))
    val wktDf: DataFrame = getWKTRowsDf
    register(spark)

    val left: Array[Any] = hexDf
      .select(
        convert_to(col("wkb"), "WKT").alias("wkt")
      )
      .collect()
      .map(_.toSeq.head)

    val right: Array[Any] = wktDf
      .select("wkt")
      .collect()
      .map(_.toSeq.head)

    right should contain allElementsOf left

    hexDf.createOrReplaceTempView("format_testing_left")
    wktDf.createOrReplaceTempView("format_testing_right")

    val left2: Array[Any] = spark.sql(
      "select convert_to_wkt(wkb) as wkt from format_testing_left"
    ).collect().map(_.toSeq.head)
    val right2: Array[Any] = spark.sql(
      "select wkt from format_testing_right"
    ).collect().map(_.toSeq.head)

    right2 should contain allElementsOf left2
  }

  ignore("Conversion from WKB to HEX") {
    val hexDf = getHexRowsDf
    val wktDf = getWKTRowsDf
      .withColumn("wkb", convert_to(col("wkt"), "wkb"))
    register(spark)

    val left = wktDf.select(
        convert_to(col("wkb"), "hex").getItem("hex").alias("hex")
      )
      .collect()
      .map(_.toSeq.head.asInstanceOf[String])
      .map(typed.hex2geom)

    val right = hexDf
      .select("hex")
      .collect()
      .map(_.toSeq.head.asInstanceOf[String])
      .map(typed.hex2geom)

    left should contain allElementsOf right

    wktDf.createOrReplaceTempView("format_testing_left")
    hexDf.createOrReplaceTempView("format_testing_right")

    val left2 = spark.sql(
        "select convert_to_hex(wkb)['hex'] as hex from format_testing_left"
      )
      .collect()
      .map(_.toSeq.head.asInstanceOf[String])
      .map(typed.hex2geom)

    val right2 = spark.sql(
        "select hex from format_testing_right"
      )
      .collect()
      .map(_.toSeq.head.asInstanceOf[String])
      .map(typed.hex2geom)

    left2 should contain allElementsOf right2
  }

  ignore("Conversion from WKB to COORDS") {
    val hexDf1 = getHexRowsDf.withColumn("test", as_hex(col("hex")))

    val hexDf = hexDf1.withColumn("coords", convert_to(as_hex(col("hex")), "coords"))
    val wktDf = getWKTRowsDf
      .withColumn("wkb", convert_to(col("wkt"), "wkb"))
    register(spark)

    val left = wktDf.select(
        convert_to(col("wkb"), "coords").alias("coords")
      )
      .collect()
      .map(_.toSeq.head)

    val right = hexDf
      .select("coords")
      .collect()
      .map(_.toSeq.head)

    left should contain allElementsOf right

    wktDf.createOrReplaceTempView("format_testing_left")
    hexDf.createOrReplaceTempView("format_testing_right")

    val left2 = spark.sql(
      "select convert_to_coords(wkb) as coords from format_testing_left"
    )
      .collect()
      .map(_.toSeq.head)

    val right2 = spark.sql(
      "select coords from format_testing_right"
    )
      .collect()
      .map(_.toSeq.head)

    left2 should contain allElementsOf right2
  }

  ignore("Conversion from WKT to WKB") {

    val hexDf: DataFrame = getHexRowsDf
      .withColumn("wkb", convert_to(as_hex(col("hex")), "WKB"))
    val wktDf: DataFrame = getWKTRowsDf
    register(spark)

    val left: Array[Any] = wktDf
      .select(
        convert_to(col("wkt"), "WKB").alias("wkb")
      )
      .collect()
      .map(_.toSeq.head)

    val right: Array[Any] = hexDf
      .select("wkb")
      .collect()
      .map(_.toSeq.head)

    left should contain allElementsOf right

    wktDf.createOrReplaceTempView("format_testing_left")
    hexDf.createOrReplaceTempView("format_testing_right")

    val left2: Array[Any] = spark.sql(
      "select convert_to_wkb(wkt) as wkt from format_testing_left"
    ).collect().map(_.toSeq.head)
    val right2: Array[Any] = spark.sql(
      "select wkb from format_testing_right"
    ).collect().map(_.toSeq.head)

    left2 should contain allElementsOf right2
  }

  ignore("Conversion from WKT to HEX") {
    val hexDf = getHexRowsDf
    val wktDf = getWKTRowsDf
    register(spark)

    val left = wktDf.select(
        convert_to(col("wkt"), "hex").getItem("hex").alias("hex")
      )
      .collect()
      .map(_.toSeq.head.asInstanceOf[String])
      .map(typed.hex2geom)

    val right = hexDf
      .select("hex")
      .collect()
      .map(_.toSeq.head.asInstanceOf[String])
      .map(typed.hex2geom)

    left should contain allElementsOf right

    wktDf.createOrReplaceTempView("format_testing_left")
    hexDf.createOrReplaceTempView("format_testing_right")

    val left2 = spark.sql(
        "select convert_to_hex(wkt)['hex'] as hex from format_testing_left"
      )
      .collect()
      .map(_.toSeq.head.asInstanceOf[String])
      .map(typed.hex2geom)

    val right2 = spark.sql(
      "select hex from format_testing_right"
    )
      .collect()
      .map(_.toSeq.head.asInstanceOf[String])
      .map(typed.hex2geom)

    left2 should contain allElementsOf right2
  }

  ignore("Conversion from WKT to COORDS") {
    val hexDf = getHexRowsDf
      .withColumn("coords", convert_to(as_hex(col("hex")), "coords"))
    val wktDf = getWKTRowsDf
    register(spark)

    val left = wktDf.select(
        convert_to(col("wkt"), "coords").alias("coords")
      )
      .collect()
      .map(_.toSeq.head)

    val right = hexDf
      .select("coords")
      .collect()
      .map(_.toSeq.head)

    left should contain allElementsOf right

    wktDf.createOrReplaceTempView("format_testing_left")
    hexDf.createOrReplaceTempView("format_testing_right")

    val left2 = spark.sql(
        "select convert_to_coords(wkt) as coords from format_testing_left"
      )
      .collect()
      .map(_.toSeq.head)

    val right2 = spark.sql(
        "select coords from format_testing_right"
      )
      .collect()
      .map(_.toSeq.head)

    left2 should contain allElementsOf right2
  }

  ignore("Conversion from HEX to WKB") {

    val hexDf: DataFrame = getHexRowsDf
      .withColumn("hex", as_hex(col("hex")))
    val wktDf: DataFrame = getWKTRowsDf
      .withColumn("wkb", convert_to(col("wkt"), "WKB"))
    register(spark)

    val left: Array[Any] = hexDf
      .select(
        convert_to(col("hex"), "WKB").alias("wkb")
      )
      .collect()
      .map(_.toSeq.head)

    val right: Array[Any] = wktDf
      .select("wkb")
      .collect()
      .map(_.toSeq.head)

    right should contain allElementsOf left

    hexDf.createOrReplaceTempView("format_testing_left")
    wktDf.createOrReplaceTempView("format_testing_right")

    val left2: Array[Any] = spark.sql(
      "select convert_to_wkb(hex) as wkt from format_testing_left"
    ).collect().map(_.toSeq.head)
    val right2: Array[Any] = spark.sql(
      "select wkb from format_testing_right"
    ).collect().map(_.toSeq.head)

    right2 should contain allElementsOf left2
  }

  ignore("Conversion from HEX to WKT") {
    val hexDf = getHexRowsDf
      .withColumn("hex", as_hex(col("hex")))
    val wktDf = getWKTRowsDf
    register(spark)

    val left = hexDf.select(
        convert_to(col("hex"), "wkt").alias("wkt").cast("string")
      )
      .collect()
      .map(_.toSeq.head.asInstanceOf[String])
      .map(typed.wkt2geom)

    val right = wktDf
      .select("wkt")
      .collect()
      .map(_.toSeq.head.asInstanceOf[String])
      .map(typed.wkt2geom)

    right should contain allElementsOf left

    hexDf.createOrReplaceTempView("format_testing_left")
    wktDf.createOrReplaceTempView("format_testing_right")

    val left2 = spark.sql(
        "select convert_to_wkt(hex) as wkt from format_testing_left"
      )
      .collect()
      .map(_.toSeq.head.asInstanceOf[String])
      .map(typed.wkt2geom)

    val right2 = spark.sql(
        "select wkt from format_testing_right"
      )
      .collect()
      .map(_.toSeq.head.asInstanceOf[String])
      .map(typed.wkt2geom)

    right2 should contain allElementsOf left2
  }

  ignore("Conversion from HEX to COORDS") {
    val hexDf = getHexRowsDf
      .withColumn("hex", as_hex(col("hex")))
    val wktDf = getWKTRowsDf
      .withColumn("coords", convert_to(col("wkt"), "coords"))
    register(spark)

    val left = hexDf.select(
        convert_to(col("hex"), "coords").alias("coords")
      )
      .collect()
      .map(_.toSeq.head)

    val right = wktDf
      .select("coords")
      .collect()
      .map(_.toSeq.head)

    right should contain allElementsOf left

    hexDf.createOrReplaceTempView("format_testing_left")
    wktDf.createOrReplaceTempView("format_testing_right")

    val left2 = spark.sql(
        "select convert_to_coords(hex) as coords from format_testing_left"
      )
      .collect()
      .map(_.toSeq.head)

    val right2 = spark.sql(
        "select coords from format_testing_right"
      )
      .collect()
      .map(_.toSeq.head)

    right2 should contain allElementsOf left2
  }

  ignore("Conversion from COORDS to WKB") {

    val hexDf: DataFrame = getHexRowsDf
      .withColumn("coords", convert_to(as_hex(col("hex")), "coords"))
    val wktDf: DataFrame = getWKTRowsDf
      .withColumn("wkb", convert_to(col("wkt"), "WKB"))
    register(spark)

    val left: Array[Any] = hexDf
      .select(
        convert_to(col("coords"), "WKB").alias("wkb")
      )
      .collect()
      .map(_.toSeq.head.asInstanceOf[Array[Byte]])
      .map(typed.wkb2geom)

    val right: Array[Any] = wktDf
      .select("wkb")
      .collect()
      .map(_.toSeq.head.asInstanceOf[Array[Byte]])
      .map(typed.wkb2geom)


    right should contain allElementsOf left

    hexDf.createOrReplaceTempView("format_testing_left")
    wktDf.createOrReplaceTempView("format_testing_right")

    val left2: Array[Any] = spark.sql(
      "select convert_to_wkb(coords) as wkb from format_testing_left"
    ).collect().map(_.toSeq.head)
    val right2: Array[Any] = spark.sql(
      "select wkb from format_testing_right"
    ).collect().map(_.toSeq.head)

    right2 should contain allElementsOf left2
  }

  ignore("Conversion from COORDS to WKT") {
    val hexDf = getHexRowsDf
      .withColumn("coords", convert_to(as_hex(col("hex")), "coords"))
    val wktDf = getWKTRowsDf
    register(spark)

    val left = hexDf.select(
        convert_to(col("coords"), "wkt").alias("wkt").cast("string")
      )
      .collect()
      .map(_.toSeq.head.asInstanceOf[String])
      .map(typed.wkt2geom)

    val right = wktDf
      .select("wkt")
      .collect()
      .map(_.toSeq.head.asInstanceOf[String])
      .map(typed.wkt2geom)

    right should contain allElementsOf left

    hexDf.createOrReplaceTempView("format_testing_left")
    wktDf.createOrReplaceTempView("format_testing_right")

    val left2 = spark.sql(
        "select convert_to_wkt(coords) as wkt from format_testing_left"
      )
      .collect()
      .map(_.toSeq.head.asInstanceOf[String])
      .map(typed.wkt2geom)

    val right2 = spark.sql(
        "select wkt from format_testing_right"
      )
      .collect()
      .map(_.toSeq.head.asInstanceOf[String])
      .map(typed.wkt2geom)

    right2 should contain allElementsOf left2
  }

  ignore("Conversion from COORDS to HEX") {
    val hexDf = getHexRowsDf
    val wktDf = getWKTRowsDf
      .withColumn("coords", convert_to(col("wkt"), "coords"))
    register(spark)

    val left = wktDf.select(
        convert_to(col("coords"), "hex").getItem("hex").alias("hex")
      )
      .collect()
      .map(_.toSeq.head.asInstanceOf[String])
      .map(typed.hex2geom)

    val right = hexDf
      .select("hex")
      .collect()
      .map(_.toSeq.head.asInstanceOf[String])
      .map(typed.hex2geom)

    left should contain allElementsOf right

    wktDf.createOrReplaceTempView("format_testing_left")
    hexDf.createOrReplaceTempView("format_testing_right")

    val left2 = spark.sql(
        "select convert_to_hex(coords)['hex'] as hex from format_testing_left"
      )
      .collect()
      .map(_.toSeq.head.asInstanceOf[String])
      .map(typed.hex2geom)

    val right2 = spark.sql(
        "select hex from format_testing_right"
      )
      .collect()
      .map(_.toSeq.head.asInstanceOf[String])
      .map(typed.hex2geom)

    left2 should contain allElementsOf right2
  }

  test("Conversion from WKB to GeoJSON") {

    val wkbDf: DataFrame = getHexRowsDf
      .select(convert_to(as_hex(col("hex")), "WKB").alias("wkb"))
    val geojsonDf: DataFrame = getGeoJSONDf
      .select(as_json(col("geojson")).alias("geojson"))
    register(spark)

    val left: Array[Any] = wkbDf
      .select(
        convert_to(col("wkb"), "geojson").alias("geojson")
      )
      .collect()
      .map(_.toSeq.head)

    // val right: Array[Any] = geojsonDf
    //   .collect()
    //   .map(_.toSeq.head)

    // right.zip(left).foreach({ case (r: String, l: String) => r should matchJson(l)})

    // wkbDf.createOrReplaceTempView("format_testing_left")
    // geojsonDf.createOrReplaceTempView("format_testing_right")

    // val left2: Array[Any] = spark.sql(
    //   "select convert_to_geojson(wkb) as geojson from format_testing_left"
    // ).collect().map(_.toSeq.head)
    // val right2: Array[Any] = spark.sql(
    //   "select geojson from format_testing_right"
    // ).collect().map(_.toSeq.head)

    // right2.zip(left2).foreach({ case (r: String, l: String) => r should matchJson(l)})
  }

  ignore("Conversion from WKT to GeoJSON") {

    val wktDf: DataFrame = getWKTRowsDf
    val geojsonDf: DataFrame = getGeoJSONDf
    register(spark)

    val left: Array[Any] = wktDf
      .select(
        convert_to(col("wkt"), "geojson").alias("geojson")
      )
      .collect()
      .map(_.toSeq.head)

    val right: Array[Any] = geojsonDf
      .collect()
      .map(_.toSeq.head)

    right.zip(left).foreach({ case (r: String, l: String) => r should matchJson(l)})

    wktDf.createOrReplaceTempView("format_testing_left")
    geojsonDf.createOrReplaceTempView("format_testing_right")

    val left2: Array[Any] = spark.sql(
      "select convert_to_geojson(wkt) as geojson from format_testing_left"
    ).collect().map(_.toSeq.head)
    val right2: Array[Any] = spark.sql(
      "select geojson from format_testing_right"
    ).collect().map(_.toSeq.head)

    right2.zip(left2).foreach({ case (r: String, l: String) => r should matchJson(l)})
  }

  ignore("Conversion from HEX to GeoJSON") {

    val hexDf: DataFrame = getHexRowsDf
      .select(as_hex(col("hex")).alias("hex"))
    val geojsonDf: DataFrame = getGeoJSONDf
    register(spark)

    val left: Array[Any] = hexDf
      .select(
        convert_to(col("hex"), "geojson").alias("geojson")
      )
      .collect()
      .map(_.toSeq.head)

    val right: Array[Any] = geojsonDf
      .collect()
      .map(_.toSeq.head)

    right.zip(left).foreach({ case (r: String, l: String) => r should matchJson(l)})

    hexDf.createOrReplaceTempView("format_testing_left")
    geojsonDf.createOrReplaceTempView("format_testing_right")

    val left2: Array[Any] = spark.sql(
      "select convert_to_geojson(hex) as geojson from format_testing_left"
    ).collect().map(_.toSeq.head)
    val right2: Array[Any] = spark.sql(
      "select geojson from format_testing_right"
    ).collect().map(_.toSeq.head)

    right2.zip(left2).foreach({ case (r: String, l: String) => r should matchJson(l)})
  }

  ignore("Conversion from COORDS to GeoJSON") {

    val coordsDf: DataFrame = getHexRowsDf
      .select(as_hex(col("hex")).alias("hex"))
      .withColumn("coords", convert_to(col("hex"), "coords"))
    val geojsonDf: DataFrame = getGeoJSONDf
    register(spark)

    val left: Array[Any] = coordsDf
      .select(
        convert_to(col("hex"), "geojson").alias("geojson")
      )
      .collect()
      .map(_.toSeq.head)

    val right: Array[Any] = geojsonDf
      .collect()
      .map(_.toSeq.head)

    right.zip(left).foreach({ case (r: String, l: String) => r should matchJson(l)})

    coordsDf.createOrReplaceTempView("format_testing_left")
    geojsonDf.createOrReplaceTempView("format_testing_right")

    val left2: Array[Any] = spark.sql(
      "select convert_to_geojson(coords) as geojson from format_testing_left"
    ).collect().map(_.toSeq.head)
    val right2: Array[Any] = spark.sql(
      "select geojson from format_testing_right"
    ).collect().map(_.toSeq.head)

    right2.zip(left2).foreach({ case (r: String, l: String) => r should matchJson(l)})
  }

  ignore("Conversion from GeoJSON to WKB") {

    val geojsonDf: DataFrame = getGeoJSONDf
    val wkbDf: DataFrame = getHexRowsDf
      .select(convert_to(col("hex"), "wkb").alias("wkb"))
    
    register(spark)

    val left: Array[Any] = geojsonDf
      .withColumn("geojson", as_json(col("geojson")))
      .select(
        convert_to(col("geojson"), "wkb").alias("wkb")
      )
      .collect()
      .map(_.toSeq.head)

    val right: Array[Any] = wkbDf
      .collect()
      .map(_.toSeq.head)

    // right.zip(left).foreach({ case (r: String, l: String) => r should matchJson(l)})

    // geojsonDf.createOrReplaceTempView("format_testing_left")
    // wkbDf.createOrReplaceTempView("format_testing_right")

    // val left2: Array[Any] = spark.sql(
    //   "select convert_to_wkb(geojson) as wkb from format_testing_left"
    // ).collect().map(_.toSeq.head)
    // val right2: Array[Any] = spark.sql(
    //   "select wkb from format_testing_right"
    // ).collect().map(_.toSeq.head)

    // right2.zip(left2).foreach({ case (r: String, l: String) => r should matchJson(l)})
  }

}