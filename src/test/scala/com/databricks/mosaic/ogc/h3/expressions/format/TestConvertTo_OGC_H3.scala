package com.databricks.mosaic.ogc.h3.expressions.format

import com.databricks.mosaic.core.geometry.MosaicGeometryOGC
import com.databricks.mosaic.core.geometry.api.GeometryAPI.OGC
import com.databricks.mosaic.core.index.H3IndexSystem
import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.mocks.{getGeoJSONDf, getHexRowsDf, getWKTRowsDf}
import com.databricks.mosaic.test.SparkTest
import com.stephenn.scalatest.jsonassert.JsonMatchers
import org.apache.spark.sql.DataFrame
import org.scalatest.{FunSuite, Matchers}

//noinspection ScalaStyle
class TestConvertTo_OGC_H3 extends FunSuite with SparkTest with Matchers with JsonMatchers {
  val mosaicContext: MosaicContext = MosaicContext(H3IndexSystem, OGC)

  import mosaicContext.functions._
  import testImplicits._

  test("Conversion from WKB to WKT") {
    mosaicContext.register(spark)

    val hexDf: DataFrame = getHexRowsDf
      .withColumn("wkb", convert_to(as_hex($"hex"), "WKB"))
    val wktDf: DataFrame = getWKTRowsDf

    val left = hexDf
      .orderBy("id")
      .select(
        convert_to($"wkb", "WKT").alias("wkt")
      )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromWKT)

    val right = wktDf
      .orderBy("id")
      .select("wkt")
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromWKT)

    right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }

    hexDf.createOrReplaceTempView("format_testing_left")
    wktDf.createOrReplaceTempView("format_testing_right")

    val left2 = spark.sql(
      "select convert_to_wkt(wkb) as wkt from format_testing_left order by id"
    )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromWKT)

    val right2 = spark
      .sql(
        "select wkt from format_testing_right order by id"
      )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromWKT)

    right2.zip(left2).foreach { case (l, r) => l.equals(r) shouldEqual true }

    val left3 = spark.sql(
      "select st_aswkt(wkb) as wkt from format_testing_left order by id"
    )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromWKT)

    right.zip(left3).foreach { case (l, r) => l.equals(r) shouldEqual true }

    val left4 = spark.sql(
      "select st_astext(wkb) as wkt from format_testing_left order by id"
    )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromWKT)

    right.zip(left4).foreach { case (l, r) => l.equals(r) shouldEqual true }
  }

  test("Conversion from WKB to HEX") {
    mosaicContext.register(spark)

    val hexDf = getHexRowsDf
    val wktDf = getWKTRowsDf
      .withColumn("wkb", convert_to($"wkt", "wkb"))

    val left = wktDf.select(
      convert_to($"wkb", "hex").getItem("hex").alias("hex")
    )
      .orderBy("id")
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromHEX)

    val right = hexDf
      .orderBy("id")
      .select("hex")
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromHEX)

    right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }

    wktDf.createOrReplaceTempView("format_testing_left")
    hexDf.createOrReplaceTempView("format_testing_right")

    val left2 = spark.sql(
      "select convert_to_hex(wkb)['hex'] as hex from format_testing_left order by id"
    )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromHEX)

    val right2 = spark.sql(
      "select hex from format_testing_right order by id"
    )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromHEX)

    right2.zip(left2).foreach { case (l, r) => l.equals(r) shouldEqual true }
  }

  test("Conversion from WKB to COORDS") {
    mosaicContext.register(spark)

    val hexDf1 = getHexRowsDf.withColumn("test", as_hex($"hex"))
    val hexDf = hexDf1.withColumn("coords", convert_to(as_hex($"hex"), "coords"))

    val wktDf = getWKTRowsDf
      .withColumn("wkb", convert_to($"wkt", "wkb"))

    val left = wktDf.select(
      convert_to($"wkb", "coords").alias("coords")
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

    val right3 = hexDf1
      .withColumn("hex", as_hex($"hex"))
      .withColumn("wkb", st_aswkb($"hex"))
      .select(st_geomfromwkb($"wkb")).alias("coords")
      .collect()
      .map(_.toSeq.head)

    right3 should contain allElementsOf left

    val left3 = spark.sql(
      "select st_geomfromwkb(wkb) as coords from format_testing_left"
    )
      .collect()
      .map(_.toSeq.head)

    left3 should contain allElementsOf right
  }

  test("Conversion from WKB to GeoJSON") {
    mosaicContext.register(spark)

    val wkbDf: DataFrame = getHexRowsDf
      .orderBy("id")
      .select(convert_to(as_hex($"hex"), "WKB").alias("wkb"))
    val geojsonDf: DataFrame = getGeoJSONDf
      .orderBy("id")
      .select(as_json($"geojson").getItem("json").alias("geojson"))

    val left = wkbDf
      .orderBy("id")
      .select(
        convert_to($"wkb", "geojson").getItem("json").alias("geojson")
      )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromJSON)
      .map(_.asInstanceOf[MosaicGeometryOGC].getGeom.getEsriGeometry) // workaround for relaxed equality and crs info missing

    val right = geojsonDf
      .orderBy("id")
      .select("geojson")
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromJSON)
      .map(_.asInstanceOf[MosaicGeometryOGC].getGeom.getEsriGeometry)

    right.zip(left).map { case (l, r) => l.equals(r) }.reduce(_ & _) shouldEqual true

    wkbDf.createOrReplaceTempView("format_testing_left")
    geojsonDf.createOrReplaceTempView("format_testing_right")

    val left2 = spark.sql(
      "select convert_to_geojson(wkb)['json'] as geojson from format_testing_left"
    )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromJSON)
      .map(_.asInstanceOf[MosaicGeometryOGC].getGeom.getEsriGeometry)


    val right2 = spark.sql(
      "select geojson from format_testing_right"
    )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromJSON)
      .map(_.asInstanceOf[MosaicGeometryOGC].getGeom.getEsriGeometry)


    right2.zip(left2).map { case (l, r) => l.equals(r) }.reduce(_ & _) shouldEqual true

    val left3 = spark.sql(
      "select st_asgeojson(wkb)['json'] as geojson from format_testing_left"
    )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromJSON)
      .map(_.asInstanceOf[MosaicGeometryOGC].getGeom.getEsriGeometry)


    right.zip(left3).map { case (l, r) => l.equals(r) }.reduce(_ & _) shouldEqual true

  }

  test("Conversion from WKT to WKB") {
    mosaicContext.register(spark)

    val hexDf: DataFrame = getHexRowsDf
      .orderBy("id")
      .withColumn("wkb", convert_to(as_hex($"hex"), "WKB"))
    val wktDf: DataFrame = getWKTRowsDf
      .orderBy("id")

    val left = wktDf
      .select(
        convert_to($"wkt", "WKB").alias("wkb")
      )
      .as[Array[Byte]]
      .collect()
      .map(MosaicGeometryOGC.fromWKB)

    val right = hexDf
      .select("wkb")
      .as[Array[Byte]]
      .collect()
      .map(MosaicGeometryOGC.fromWKB)

    right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }

    wktDf.createOrReplaceTempView("format_testing_left")
    hexDf.createOrReplaceTempView("format_testing_right")

    val left2 = spark.sql(
      "select convert_to_wkb(wkt) as wkb from format_testing_left"
    )
      .as[Array[Byte]]
      .collect()
      .map(MosaicGeometryOGC.fromWKB)

    val right2 = spark.sql(
      "select wkb from format_testing_right"
    )
      .as[Array[Byte]]
      .collect()
      .map(MosaicGeometryOGC.fromWKB)

    right2.zip(left2).foreach { case (l, r) => l.equals(r) shouldEqual true }

    val left3 = spark.sql(
      "select st_aswkb(wkt) as wkb from format_testing_left"
    )
      .as[Array[Byte]]
      .collect()
      .map(MosaicGeometryOGC.fromWKB)

    right.zip(left3).foreach { case (l, r) => l.equals(r) shouldEqual true }

    val left4 = spark.sql(
      "select st_asbinary(wkt) as wkb from format_testing_left"
    )
      .as[Array[Byte]]
      .collect()
      .map(MosaicGeometryOGC.fromWKB)

    right.zip(left4).foreach { case (l, r) => l.equals(r) shouldEqual true }
  }

  test("Conversion from WKT to HEX") {
    mosaicContext.register(spark)

    val hexDf = getHexRowsDf.orderBy("id")
    val wktDf = getWKTRowsDf.orderBy("id")

    val left = wktDf.select(
      convert_to($"wkt", "hex").getItem("hex").alias("hex")
    )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromHEX)

    val right = hexDf
      .select("hex")
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromHEX)

    right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }

    wktDf.createOrReplaceTempView("format_testing_left")
    hexDf.createOrReplaceTempView("format_testing_right")

    val left2 = spark.sql(
      "select convert_to_hex(wkt)['hex'] as hex from format_testing_left"
    )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromHEX)

    val right2 = spark.sql(
      "select hex from format_testing_right"
    )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromHEX)

    right2.zip(left2).foreach { case (l, r) => l.equals(r) shouldEqual true }
  }

  test("Conversion from WKT to COORDS") {
    mosaicContext.register(spark)

    val hexDf = getHexRowsDf
      .withColumn("coords", convert_to(as_hex($"hex"), "coords"))
    val wktDf = getWKTRowsDf

    val left = wktDf.select(
      convert_to($"wkt", "coords").alias("coords")
    )
      .collect()
      .map(_.toSeq.head)

    val right = hexDf
      .select("coords")
      .collect()
      .map(_.toSeq.head)

    right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }

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

    right2.zip(left2).foreach { case (l, r) => l.equals(r) shouldEqual true }

    val left3 = wktDf.select(
      st_geomfromwkt($"wkt").alias("coords")
    )
      .collect()
      .map(_.toSeq.head)

    right.zip(left3).foreach { case (l, r) => l.equals(r) shouldEqual true }

    val left4 = spark.sql(
      "select st_geomfromwkt(wkt) as coords from format_testing_left"
    )
      .collect()
      .map(_.toSeq.head)

    right.zip(left4).foreach { case (l, r) => l.equals(r) shouldEqual true }

  }

  test("Conversion from WKT to GeoJSON") {
    mosaicContext.register(spark)

    val wktDf: DataFrame = getWKTRowsDf.orderBy("id")
    val geojsonDf: DataFrame = getGeoJSONDf.orderBy("id")
      .select(as_json($"geojson").getItem("json").alias("geojson"))

    val left = wktDf
      .select(
        convert_to($"wkt", "geojson").getItem("json").alias("geojson")
      )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromJSON)
      .map(_.asInstanceOf[MosaicGeometryOGC].getGeom.getEsriGeometry) // workaround for missing crs in test data

    val right = geojsonDf
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromJSON)
      .map(_.asInstanceOf[MosaicGeometryOGC].getGeom.getEsriGeometry)


    right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }

    wktDf.createOrReplaceTempView("format_testing_left")
    geojsonDf.createOrReplaceTempView("format_testing_right")

    val left2 = spark.sql(
      "select convert_to_geojson(wkt)['json'] as geojson from format_testing_left"
    )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromJSON)
      .map(_.asInstanceOf[MosaicGeometryOGC].getGeom.getEsriGeometry)

    val right2 = spark.sql(
      "select geojson from format_testing_right"
    )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromJSON)
      .map(_.asInstanceOf[MosaicGeometryOGC].getGeom.getEsriGeometry)


    right2.zip(left2).foreach { case (l, r) => l.equals(r) shouldEqual true }
  }

  test("Conversion from HEX to WKB") {
    mosaicContext.register(spark)

    val hexDf: DataFrame = getHexRowsDf.orderBy("id")
      .withColumn("hex", as_hex($"hex"))
    val wktDf: DataFrame = getWKTRowsDf.orderBy("id")
      .withColumn("wkb", convert_to($"wkt", "WKB"))

    val left = hexDf
      .select(
        convert_to($"hex", "WKB").alias("wkb")
      )
      .as[Array[Byte]]
      .collect()
      .map(MosaicGeometryOGC.fromWKB)

    val right = wktDf
      .select("wkb")
      .as[Array[Byte]]
      .collect()
      .map(MosaicGeometryOGC.fromWKB)

    right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }

    hexDf.createOrReplaceTempView("format_testing_left")
    wktDf.createOrReplaceTempView("format_testing_right")

    val left2 = spark.sql(
      "select convert_to_wkb(hex) as wkt from format_testing_left"
    )
      .as[Array[Byte]]
      .collect()
      .map(MosaicGeometryOGC.fromWKB)

    val right2 = spark.sql(
      "select wkb from format_testing_right"
    )
      .as[Array[Byte]]
      .collect()
      .map(MosaicGeometryOGC.fromWKB)

    right2.zip(left2).foreach { case (l, r) => l.equals(r) shouldEqual true }
  }

  test("Conversion from HEX to WKT") {
    mosaicContext.register(spark)

    val hexDf = getHexRowsDf.orderBy("id")
      .withColumn("hex", as_hex($"hex"))
    val wktDf = getWKTRowsDf.orderBy("id")

    val left = hexDf.select(
      convert_to($"hex", "wkt").alias("wkt").cast("string")
    )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromWKT)

    val right = wktDf
      .select("wkt")
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromWKT)

    right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }

    hexDf.createOrReplaceTempView("format_testing_left")
    wktDf.createOrReplaceTempView("format_testing_right")

    val left2 = spark.sql(
      "select convert_to_wkt(hex) as wkt from format_testing_left"
    )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromWKT)

    val right2 = spark.sql(
      "select wkt from format_testing_right"
    )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromWKT)

    right2.zip(left2).foreach { case (l, r) => l.equals(r) shouldEqual true }
  }

  test("Conversion from HEX to COORDS") {
    mosaicContext.register(spark)

    val hexDf = getHexRowsDf.orderBy("id")
      .withColumn("hex", as_hex($"hex"))
    val wktDf = getWKTRowsDf.orderBy("id")
      .withColumn("coords", convert_to($"wkt", "coords"))

    val left = hexDf.select(
      convert_to($"hex", "coords").alias("coords")
    )
      .collect()
      .map(_.toSeq.head)

    val right = wktDf
      .select("coords")
      .collect()
      .map(_.toSeq.head)

    right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }

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

    right2.zip(left2).foreach { case (l, r) => l.equals(r) shouldEqual true }
  }

  test("Conversion from HEX to GeoJSON") {
    mosaicContext.register(spark)

    val hexDf: DataFrame = getHexRowsDf.orderBy("id")
      .select(as_hex($"hex").alias("hex"))
    val geojsonDf: DataFrame = getGeoJSONDf.orderBy("id")

    val left = hexDf
      .select(
        convert_to($"hex", "geojson").getItem("json").alias("geojson")
      )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromJSON)
      .map(_.asInstanceOf[MosaicGeometryOGC].getGeom.getEsriGeometry) // workaround for missing crs info in test data

    val right = geojsonDf
      .select("geojson")
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromJSON)
      .map(_.asInstanceOf[MosaicGeometryOGC].getGeom.getEsriGeometry)

    right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }

    hexDf.createOrReplaceTempView("format_testing_left")
    geojsonDf.createOrReplaceTempView("format_testing_right")

    val left2 = spark.sql(
      "select convert_to_geojson(hex)['json'] as geojson from format_testing_left"
    )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromJSON)
      .map(_.asInstanceOf[MosaicGeometryOGC].getGeom.getEsriGeometry)

    val right2 = spark.sql(
      "select geojson from format_testing_right"
    )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromJSON)
      .map(_.asInstanceOf[MosaicGeometryOGC].getGeom.getEsriGeometry)

    right2.zip(left2).foreach { case (l, r) => l.equals(r) shouldEqual true }
  }

  test("Conversion from COORDS to WKB") {
    mosaicContext.register(spark)

    val hexDf: DataFrame = getHexRowsDf.orderBy("id")
      .withColumn("coords", convert_to(as_hex($"hex"), "coords"))
    val wkbDf: DataFrame = getWKTRowsDf.orderBy("id")
      .withColumn("wkb", convert_to($"wkt", "WKB"))

    val left = hexDf
      .select(
        convert_to($"coords", "WKB").alias("wkb")
      )
      .as[Array[Byte]]
      .collect()
      .map(MosaicGeometryOGC.fromWKB)

    val right = wkbDf
      .select("wkb")
      .as[Array[Byte]]
      .collect()
      .map(MosaicGeometryOGC.fromWKB)


    right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }

    hexDf.createOrReplaceTempView("format_testing_left")
    wkbDf.createOrReplaceTempView("format_testing_right")

    val left2 = spark.sql(
      "select convert_to_wkb(coords) as wkb from format_testing_left"
    )
      .as[Array[Byte]]
      .collect()
      .map(MosaicGeometryOGC.fromWKB)

    val right2 = spark.sql(
      "select wkb from format_testing_right"
    )
      .as[Array[Byte]]
      .collect()
      .map(MosaicGeometryOGC.fromWKB)

    right2.zip(left2).foreach { case (l, r) => l.equals(r) shouldEqual true }

    val left3 = hexDf
      .select(st_asbinary($"coords").alias("wkb"))
      .as[Array[Byte]]
      .collect()
      .map(MosaicGeometryOGC.fromWKB)

    right.zip(left3).foreach { case (l, r) => l.equals(r) shouldEqual true }

    val left4 = hexDf
      .select(st_aswkb($"coords").alias("wkb"))
      .as[Array[Byte]]
      .collect()
      .map(MosaicGeometryOGC.fromWKB)

    right.zip(left4).foreach { case (l, r) => l.equals(r) shouldEqual true }

  }

  test("Conversion from COORDS to WKT") {
    mosaicContext.register(spark)

    val hexDf = getHexRowsDf.orderBy("id")
      .withColumn("coords", convert_to(as_hex($"hex"), "coords"))
    val wktDf = getWKTRowsDf.orderBy("id")

    val left = hexDf.select(
      convert_to($"coords", "wkt").alias("wkt").cast("string")
    )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromWKT)

    val right = wktDf
      .select("wkt")
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromWKT)

    right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }

    hexDf.createOrReplaceTempView("format_testing_left")
    wktDf.createOrReplaceTempView("format_testing_right")

    val left2 = spark.sql(
      "select convert_to_wkt(coords) as wkt from format_testing_left"
    )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromWKT)

    val right2 = spark.sql(
      "select wkt from format_testing_right"
    )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromWKT)

    right2.zip(left2).foreach { case (l, r) => l.equals(r) shouldEqual true }

    val left3 = hexDf.select(
      st_astext($"coords").alias("wkt")
    )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromWKT)

    right.zip(left3).foreach { case (l, r) => l.equals(r) shouldEqual true }

    val left4 = hexDf.select(
      st_aswkt($"coords").alias("wkt")
    )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromWKT)

    right.zip(left4).foreach { case (l, r) => l.equals(r) shouldEqual true }
  }

  test("Conversion from COORDS to HEX") {
    mosaicContext.register(spark)

    val hexDf = getHexRowsDf.orderBy("id")
    val wktDf = getWKTRowsDf.orderBy("id")
      .withColumn("coords", convert_to($"wkt", "coords"))

    val left = wktDf.select(
      convert_to($"coords", "hex").getItem("hex").alias("hex")
    )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromHEX)

    val right = hexDf
      .select("hex")
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromHEX)

    right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }

    wktDf.createOrReplaceTempView("format_testing_left")
    hexDf.createOrReplaceTempView("format_testing_right")

    val left2 = spark.sql(
      "select convert_to_hex(coords)['hex'] as hex from format_testing_left"
    )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromHEX)

    val right2 = spark.sql(
      "select hex from format_testing_right"
    )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromHEX)

    right2.zip(left2).foreach { case (l, r) => l.equals(r) shouldEqual true }
  }

  test("Conversion from COORDS to GeoJSON") {
    mosaicContext.register(spark)

    val coordsDf: DataFrame = getHexRowsDf.orderBy("id")
      .select(as_hex($"hex").alias("hex"))
      .withColumn("coords", convert_to($"hex", "coords"))
    val geojsonDf: DataFrame = getGeoJSONDf.orderBy("id")

    val left = coordsDf
      .select(
        convert_to($"coords", "geojson").getItem("json").alias("geojson")
      )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromJSON)
      .map(_.asInstanceOf[MosaicGeometryOGC].getGeom.getEsriGeometry)

    val right = geojsonDf
      .select("geojson")
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromJSON)
      .map(_.asInstanceOf[MosaicGeometryOGC].getGeom.getEsriGeometry)

    right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }

    coordsDf.createOrReplaceTempView("format_testing_left")
    geojsonDf.createOrReplaceTempView("format_testing_right")

    val left2 = spark.sql(
      "select convert_to_geojson(coords)['json'] as geojson from format_testing_left"
    )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromJSON)
      .map(_.asInstanceOf[MosaicGeometryOGC].getGeom.getEsriGeometry)

    val right2 = spark.sql(
      "select geojson from format_testing_right"
    )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromJSON)
      .map(_.asInstanceOf[MosaicGeometryOGC].getGeom.getEsriGeometry)

    right2.zip(left2).foreach { case (l, r) => l.equals(r) shouldEqual true }

    val left3 = coordsDf
      .select(
        st_asgeojson($"coords").getItem("json").alias("geojson")
      )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromJSON)
      .map(_.asInstanceOf[MosaicGeometryOGC].getGeom.getEsriGeometry)

    right.zip(left3).foreach { case (l, r) => l.equals(r) shouldEqual true }

  }

  test("Conversion from GeoJSON to WKB") {
    mosaicContext.register(spark)

    val geojsonDf: DataFrame = getGeoJSONDf.orderBy("id")
    val wkbDf: DataFrame = getHexRowsDf.orderBy("id")
      .select(as_hex($"hex").alias("hex"))
      .select(convert_to($"hex", "wkb").alias("wkb"))

    val left = geojsonDf
      .withColumn("geojson", as_json($"geojson"))
      .select(
        convert_to($"geojson", "wkb").alias("wkb")
      )
      .as[Array[Byte]]
      .collect()
      .map(MosaicGeometryOGC.fromWKB)

    val right = wkbDf
      .as[Array[Byte]]
      .collect()
      .map(MosaicGeometryOGC.fromWKB)

    right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }

    geojsonDf.createOrReplaceTempView("format_testing_left")
    wkbDf.createOrReplaceTempView("format_testing_right")

    val left2 = spark.sql(
      "select convert_to_wkb(as_json(geojson)) as wkb from format_testing_left"
    )
      .as[Array[Byte]]
      .collect()
      .map(MosaicGeometryOGC.fromWKB)

    val right2 = spark.sql(
      "select wkb from format_testing_right"
    )
      .as[Array[Byte]]
      .collect()
      .map(MosaicGeometryOGC.fromWKB)

    right2.zip(left2).foreach { case (l, r) => l.equals(r) shouldEqual true }
  }

  test("Conversion from GeoJSON to WKT") {
    mosaicContext.register(spark)

    val geojsonDf: DataFrame = getGeoJSONDf.orderBy("id")
    val wktDf: DataFrame = getWKTRowsDf.orderBy("id")

    val left = geojsonDf
      .withColumn("geojson", as_json($"geojson"))
      .select(
        convert_to($"geojson", "wkt").alias("wkt")
      )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromWKT)

    val right = wktDf
      .select("wkt")
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromWKT)

    right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }

    geojsonDf.createOrReplaceTempView("format_testing_left")
    wktDf.createOrReplaceTempView("format_testing_right")

    val left2 = spark.sql(
      "select convert_to_wkt(as_json(geojson)) as wkt from format_testing_left"
    )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromWKT)

    val right2 = spark.sql(
      "select wkt from format_testing_right"
    )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromWKT)

    right2.zip(left2).foreach { case (l, r) => l.equals(r) shouldEqual true }
  }

  test("Conversion from GeoJSON to HEX") {
    mosaicContext.register(spark)

    val geojsonDf: DataFrame = getGeoJSONDf.orderBy("id")
    val hexDf: DataFrame = getHexRowsDf.orderBy("id")

    val left = geojsonDf
      .withColumn("geojson", as_json($"geojson"))
      .select(
        convert_to($"geojson", "hex").getItem("hex").alias("hex")
      )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromHEX)

    val right = hexDf
      .select("hex")
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromHEX)

    right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }

    geojsonDf.createOrReplaceTempView("format_testing_left")
    hexDf.createOrReplaceTempView("format_testing_right")

    val left2 = spark.sql(
      "select convert_to_hex(as_json(geojson))['hex'] as hex from format_testing_left"
    )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromHEX)

    val right2 = spark.sql(
      "select hex from format_testing_right"
    )
      .as[String]
      .collect()
      .map(MosaicGeometryOGC.fromHEX)

    right2.zip(left2).foreach { case (l, r) => l.equals(r) shouldEqual true }
  }

  test("Conversion from GeoJSON to COORDS") {
    mosaicContext.register(spark)

    val geojsonDf: DataFrame = getGeoJSONDf
    val coordsDf: DataFrame = getHexRowsDf
      .select(as_hex($"hex").alias("hex"))
      .select(convert_to($"hex", "coords").alias("coords"))

    val left = geojsonDf
      .withColumn("geojson", as_json($"geojson"))
      .select(
        convert_to($"geojson", "coords").alias("coords")
      )
      .collect()
      .map(_.toSeq.head)

    val right = coordsDf
      .collect()
      .map(_.toSeq.head)

    right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }

    geojsonDf.createOrReplaceTempView("format_testing_left")
    coordsDf.createOrReplaceTempView("format_testing_right")

    val left2 = spark.sql(
      "select convert_to_coords(as_json(geojson)) as coords from format_testing_left"
    )
      .collect()
      .map(_.toSeq.head)
    val right2 = spark.sql(
      "select coords from format_testing_right"
    )
      .collect()
      .map(_.toSeq.head)

    right2.zip(left2).foreach { case (l, r) => l.equals(r) shouldEqual true }

    val left3 = geojsonDf.select(
      st_geomfromgeojson($"geojson").alias("coords")
    )
      .collect()
      .map(_.toSeq.head)

    right.zip(left3).foreach { case (l, r) => l.equals(r) shouldEqual true }

    val left4 = spark.sql(
      "select st_geomfromgeojson(geojson) as coords from format_testing_left"
    )
      .collect()
      .map(_.toSeq.head)

    right.zip(left4).foreach { case (l, r) => l.equals(r) shouldEqual true }
  }

}