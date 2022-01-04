package com.databricks.mosaic.jts.h3.expressions.format

import com.databricks.mosaic.core.geometry.MosaicGeometryJTS
import com.databricks.mosaic.core.geometry.api.GeometryAPI.JTS
import com.databricks.mosaic.core.index.H3IndexSystem
import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.mocks.{getGeoJSONDf, getHexRowsDf, getWKTRowsDf}
import com.databricks.mosaic.test.SparkTest
import com.stephenn.scalatest.jsonassert.JsonMatchers
import org.apache.spark.sql.DataFrame
import org.scalatest.{FunSuite, Matchers}

class TestConvertTo_JTS_H3 extends FunSuite with SparkTest with Matchers with JsonMatchers {
  val mosaicContext: MosaicContext = MosaicContext(H3IndexSystem, JTS)
  import mosaicContext.functions._

  test("Conversion from WKB to WKT") {
    mosaicContext.register(spark)
    val ss = spark
    import ss.implicits._

    val hexDf: DataFrame = getHexRowsDf
      .withColumn("wkb", convert_to(as_hex($"hex"), "WKB"))
    val wktDf: DataFrame = getWKTRowsDf
    
    val left: Array[Any] = hexDf
      .select(
        convert_to($"wkb", "WKT").alias("wkt")
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

    val left3 = spark.sql(
      "select st_aswkt(wkb) as wkt from format_testing_left"
    ).collect.map(_.toSeq.head)

    left3 should contain allElementsOf right

    val left4 = spark.sql(
      "select st_astext(wkb) as wkt from format_testing_left"
    ).collect.map(_.toSeq.head)

    left4 should contain allElementsOf right
  }

  test("Conversion from WKB to HEX") {
    mosaicContext.register(spark)
    val ss = spark
    import ss.implicits._

    val hexDf = getHexRowsDf
    val wktDf = getWKTRowsDf
      .withColumn("wkb", convert_to($"wkt", "wkb"))

    val left = wktDf.select(
        convert_to($"wkb", "hex").getItem("hex").alias("hex")
      )
      .collect()
      .map(_.toSeq.head.asInstanceOf[String])
      .map(MosaicGeometryJTS.fromHEX)
      .map(_.asInstanceOf[MosaicGeometryJTS].geom)

    val right = hexDf
      .select("hex")
      .collect()
      .map(_.toSeq.head.asInstanceOf[String])
      .map(MosaicGeometryJTS.fromHEX)
      .map(_.asInstanceOf[MosaicGeometryJTS].geom)

    left should contain allElementsOf right

    wktDf.createOrReplaceTempView("format_testing_left")
    hexDf.createOrReplaceTempView("format_testing_right")

    val left2 = spark.sql(
        "select convert_to_hex(wkb)['hex'] as hex from format_testing_left"
      )
      .collect()
      .map(_.toSeq.head.asInstanceOf[String])
      .map(MosaicGeometryJTS.fromHEX)
      .map(_.asInstanceOf[MosaicGeometryJTS].geom)

    val right2 = spark.sql(
        "select hex from format_testing_right"
      )
      .collect()
      .map(_.toSeq.head.asInstanceOf[String])
      .map(MosaicGeometryJTS.fromHEX)
      .map(_.asInstanceOf[MosaicGeometryJTS].geom)

    left2 should contain allElementsOf right2
  }

  test("Conversion from WKB to COORDS") {
    mosaicContext.register(spark)
    val ss = spark
    import ss.implicits._

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
    val ss = spark
    import ss.implicits._

    val wkbDf: DataFrame = getHexRowsDf
      .select(convert_to(as_hex($"hex"), "WKB").alias("wkb"))
    val geojsonDf: DataFrame = getGeoJSONDf
      .select(as_json($"geojson").getItem("json").alias("geojson"))

    val left: Array[String] = wkbDf.select(
        convert_to($"wkb", "geojson").getItem("json").alias("geojson")
      )
      .orderBy("geojson")
      .as[String]
      .collect()

    val right: Array[String] = geojsonDf
      .orderBy("geojson")
      .as[String]
      .collect()

    right.zip(left).foreach({ case (r: String, l: String) => r should matchJson(l)})

    wkbDf.createOrReplaceTempView("format_testing_left")
    geojsonDf.createOrReplaceTempView("format_testing_right")

    val left2: Array[String] = spark.sql(
      "select convert_to_geojson(wkb)['json'] as geojson from format_testing_left"
    )
    .orderBy("geojson")
    .as[String]
    .collect()

    val right2: Array[String] = spark.sql(
      "select geojson from format_testing_right"
    )
    .orderBy("geojson")
    .as[String]
    .collect()

    right2.zip(left2).foreach({ case (r: String, l: String) => r should matchJson(l)})

    val left3 = spark.sql(
      "select st_asgeojson(wkb)['json'] as geojson from format_testing_left"
    )
    .orderBy("geojson")
    .as[String]
    .collect()

    right.zip(left3).foreach({ case (r: String, l: String) => r should matchJson(l)})

  }

  test("Conversion from WKT to WKB") {
    mosaicContext.register(spark)
    val ss = spark
    import ss.implicits._

    val hexDf: DataFrame = getHexRowsDf
      .withColumn("wkb", convert_to(as_hex($"hex"), "WKB"))
    val wktDf: DataFrame = getWKTRowsDf

    val left: Array[Any] = wktDf
      .select(
        convert_to($"wkt", "WKB").alias("wkb")
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
      "select convert_to_wkb(wkt) as wkb from format_testing_left"
    ).collect().map(_.toSeq.head)
    val right2: Array[Any] = spark.sql(
      "select wkb from format_testing_right"
    ).collect().map(_.toSeq.head)

    left2 should contain allElementsOf right2

    val left3 = spark.sql(
      "select st_aswkb(wkt) as wkb from format_testing_left"
    ).collect().map(_.toSeq.head)

    left3 should contain allElementsOf right

    val left4 = spark.sql(
      "select st_asbinary(wkt) as wkb from format_testing_left"
    ).collect().map(_.toSeq.head)

    left4 should contain allElementsOf right
  }

  test("Conversion from WKT to HEX") {
    mosaicContext.register(spark)
    val ss = spark
    import ss.implicits._

    val hexDf = getHexRowsDf
    val wktDf = getWKTRowsDf

    val left = wktDf.select(
        convert_to($"wkt", "hex").getItem("hex").alias("hex")
      )
      .collect()
      .map(_.toSeq.head.asInstanceOf[String])
      .map(MosaicGeometryJTS.fromHEX)
      .map(_.asInstanceOf[MosaicGeometryJTS].geom)

    val right = hexDf
      .select("hex")
      .collect()
      .map(_.toSeq.head.asInstanceOf[String])
      .map(MosaicGeometryJTS.fromHEX)
      .map(_.asInstanceOf[MosaicGeometryJTS].geom)

    left should contain allElementsOf right

    wktDf.createOrReplaceTempView("format_testing_left")
    hexDf.createOrReplaceTempView("format_testing_right")

    val left2 = spark.sql(
        "select convert_to_hex(wkt)['hex'] as hex from format_testing_left"
      )
      .collect()
      .map(_.toSeq.head.asInstanceOf[String])
      .map(MosaicGeometryJTS.fromHEX)
      .map(_.asInstanceOf[MosaicGeometryJTS].geom)

    val right2 = spark.sql(
      "select hex from format_testing_right"
    )
      .collect()
      .map(_.toSeq.head.asInstanceOf[String])
      .map(MosaicGeometryJTS.fromHEX)
      .map(_.asInstanceOf[MosaicGeometryJTS].geom)

    left2 should contain allElementsOf right2
  }

  test("Conversion from WKT to COORDS") {
    mosaicContext.register(spark)
    val ss = spark
    import ss.implicits._

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

    val left3 = wktDf.select(
        st_geomfromwkt($"wkt").alias("coords")
      )
      .collect()
      .map(_.toSeq.head)

    left3 should contain allElementsOf right

    val left4 = spark.sql(
        "select st_geomfromwkt(wkt) as coords from format_testing_left"
      )
      .collect()
      .map(_.toSeq.head)

    left4 should contain allElementsOf right

  }

  test("Conversion from WKT to GeoJSON") {
    mosaicContext.register(spark)
    val ss = spark
    import ss.implicits._

    val wktDf: DataFrame = getWKTRowsDf
    val geojsonDf: DataFrame = getGeoJSONDf
      .select(as_json($"geojson").getItem("json").alias("geojson"))

    val left: Array[String] = wktDf
      .select(
        convert_to($"wkt", "geojson").getItem("json").alias("geojson")
      )
      .orderBy("geojson")
      .as[String]
      .collect()

    val right: Array[String] = geojsonDf
      .as[String]
      .orderBy("geojson")
      .collect()

    right.zip(left).foreach({ case (r: String, l: String) => r should matchJson(l)})

    wktDf.createOrReplaceTempView("format_testing_left")
    geojsonDf.createOrReplaceTempView("format_testing_right")

    val left2: Array[String] = spark.sql(
      "select convert_to_geojson(wkt)['json'] as geojson from format_testing_left"
    )
    .orderBy("geojson")
    .as[String]
    .collect()
    
    val right2: Array[String] = spark.sql(
      "select geojson from format_testing_right"
    )
    .orderBy("geojson")
    .as[String]
    .collect()

    right2.zip(left2).foreach({ case (r: String, l: String) => r should matchJson(l)})
  }

  test("Conversion from HEX to WKB") {
    mosaicContext.register(spark)
    val ss = spark
    import ss.implicits._

    val hexDf: DataFrame = getHexRowsDf
      .withColumn("hex", as_hex($"hex"))
    val wktDf: DataFrame = getWKTRowsDf
      .withColumn("wkb", convert_to($"wkt", "WKB"))

    val left: Array[Any] = hexDf
      .select(
        convert_to($"hex", "WKB").alias("wkb")
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

  test("Conversion from HEX to WKT") {
    mosaicContext.register(spark)
    val ss = spark
    import ss.implicits._

    val hexDf = getHexRowsDf
      .withColumn("hex", as_hex($"hex"))
    val wktDf = getWKTRowsDf

    val left = hexDf.select(
        convert_to($"hex", "wkt").alias("wkt").cast("string")
      )
      .collect()
      .map(_.toSeq.head.asInstanceOf[String])
      .map(MosaicGeometryJTS.fromWKT)
      .map(_.asInstanceOf[MosaicGeometryJTS].geom)

    val right = wktDf
      .select("wkt")
      .collect()
      .map(_.toSeq.head.asInstanceOf[String])
      .map(MosaicGeometryJTS.fromWKT)
      .map(_.asInstanceOf[MosaicGeometryJTS].geom)

    right should contain allElementsOf left

    hexDf.createOrReplaceTempView("format_testing_left")
    wktDf.createOrReplaceTempView("format_testing_right")

    val left2 = spark.sql(
        "select convert_to_wkt(hex) as wkt from format_testing_left"
      )
      .collect()
      .map(_.toSeq.head.asInstanceOf[String])
      .map(MosaicGeometryJTS.fromWKT)
      .map(_.asInstanceOf[MosaicGeometryJTS].geom)

    val right2 = spark.sql(
        "select wkt from format_testing_right"
      )
      .collect()
      .map(_.toSeq.head.asInstanceOf[String])
      .map(MosaicGeometryJTS.fromWKT)
      .map(_.asInstanceOf[MosaicGeometryJTS].geom)

    right2 should contain allElementsOf left2
  }

  test("Conversion from HEX to COORDS") {
    mosaicContext.register(spark)
    val ss = spark
    import ss.implicits._

    val hexDf = getHexRowsDf
      .withColumn("hex", as_hex($"hex"))
    val wktDf = getWKTRowsDf
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

  test("Conversion from HEX to GeoJSON") {
    mosaicContext.register(spark)
    val ss = spark
    import ss.implicits._

    val hexDf: DataFrame = getHexRowsDf.orderBy("id")
      .select(as_hex($"hex").alias("hex"))
    val geojsonDf: DataFrame = getGeoJSONDf.orderBy("id")

    val left: Array[String] = hexDf
      .select(
        convert_to($"hex", "geojson").getItem("json").alias("geojson")
      )
      .as[String]
      .collect()

    val right: Array[String] = geojsonDf
      .select("geojson")
      .as[String]
      .collect()

    right.zip(left).foreach({ case (r: String, l: String) => r should matchJson(l)})

    hexDf.createOrReplaceTempView("format_testing_left")
    geojsonDf.createOrReplaceTempView("format_testing_right")

    val left2: Array[String] = spark.sql(
        "select convert_to_geojson(hex)['json'] as geojson from format_testing_left"
      )
      .as[String]
      .collect()

    val right2: Array[String] = spark.sql(
        "select geojson from format_testing_right"
      )
      .as[String]
      .collect()

    right2.zip(left2).foreach({ case (r: String, l: String) => r should matchJson(l)})
  }

  test("Conversion from COORDS to WKB") {
    mosaicContext.register(spark)
    val ss = spark
    import ss.implicits._

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
      .map(MosaicGeometryJTS.fromWKB)

    val right = wkbDf
      .select("wkb")
      .as[Array[Byte]]
      .collect()
      .map(MosaicGeometryJTS.fromWKB)

    right.zip(left).foreach{ case (l, r) => l.equals(r) shouldEqual true }

    hexDf.createOrReplaceTempView("format_testing_left")
    wkbDf.createOrReplaceTempView("format_testing_right")

    val left2 = spark.sql(
        "select convert_to_wkb(coords) as wkb from format_testing_left"
      )
      .as[Array[Byte]]
      .collect()
      .map(MosaicGeometryJTS.fromWKB)

    val right2 = spark.sql(
        "select wkb from format_testing_right"
      )
      .as[Array[Byte]]
      .collect()
      .map(MosaicGeometryJTS.fromWKB)

    right2.zip(left2).foreach{ case (l, r) => l.equals(r) shouldEqual true }

    val left3  = hexDf
      .select(st_asbinary($"coords").alias("wkb"))
      .as[Array[Byte]]
      .collect()
      .map(MosaicGeometryJTS.fromWKB)

    right.zip(left3).foreach{ case (l, r) => l.equals(r) shouldEqual true }

    val left4  = hexDf
      .select(st_aswkb($"coords").alias("wkb"))
      .as[Array[Byte]]
      .collect()
      .map(MosaicGeometryJTS.fromWKB)

    right.zip(left4).foreach{ case (l, r) => l.equals(r) shouldEqual true }

  }

  test("Conversion from COORDS to WKT") {
    mosaicContext.register(spark)
    val ss = spark
    import ss.implicits._

    val hexDf = getHexRowsDf.orderBy("id")
      .withColumn("coords", convert_to(as_hex($"hex"), "coords"))
    val wktDf = getWKTRowsDf.orderBy("id")

    val left = hexDf.select(
        convert_to($"coords", "wkt").alias("wkt").cast("string")
      )
      .as[String]
      .collect()
      .map(MosaicGeometryJTS.fromWKT)

    val right = wktDf
      .select("wkt")
      .as[String]
      .collect()
      .map(MosaicGeometryJTS.fromWKT)

    right.zip(left).foreach{ case (l, r) => l.equals(r) shouldEqual true }

    hexDf.createOrReplaceTempView("format_testing_left")
    wktDf.createOrReplaceTempView("format_testing_right")

    val left2 = spark.sql(
        "select convert_to_wkt(coords) as wkt from format_testing_left"
      )
      .as[String]
      .collect()
      .map(MosaicGeometryJTS.fromWKT)

    val right2 = spark.sql(
        "select wkt from format_testing_right"
      )
      .as[String]
      .collect()
      .map(MosaicGeometryJTS.fromWKT)

    right2.zip(left2).foreach{ case (l, r) => l.equals(r) shouldEqual true }

    val left3 = hexDf.select(
        st_astext($"coords").alias("wkt")
      )
      .as[String]
      .collect()
      .map(MosaicGeometryJTS.fromWKT)

    right.zip(left3).foreach{ case (l, r) => l.equals(r) shouldEqual true }

    val left4 = hexDf.select(
        st_aswkt($"coords").alias("wkt")
      )
      .as[String]
      .collect()
      .map(MosaicGeometryJTS.fromWKT)

    right.zip(left4).foreach{ case (l, r) => l.equals(r) shouldEqual true }
  }

  test("Conversion from COORDS to HEX") {
    mosaicContext.register(spark)
    val ss = spark
    import ss.implicits._

    val hexDf = getHexRowsDf
    val wktDf = getWKTRowsDf
      .withColumn("coords", convert_to($"wkt", "coords"))

    val left = wktDf.select(
        convert_to($"coords", "hex").getItem("hex").alias("hex")
      )
      .collect()
      .map(_.toSeq.head.asInstanceOf[String])
      .map(MosaicGeometryJTS.fromHEX)
      .map(_.asInstanceOf[MosaicGeometryJTS].geom)

    val right = hexDf
      .select("hex")
      .collect()
      .map(_.toSeq.head.asInstanceOf[String])
      .map(MosaicGeometryJTS.fromHEX)
      .map(_.asInstanceOf[MosaicGeometryJTS].geom)

    left should contain allElementsOf right

    wktDf.createOrReplaceTempView("format_testing_left")
    hexDf.createOrReplaceTempView("format_testing_right")

    val left2 = spark.sql(
        "select convert_to_hex(coords)['hex'] as hex from format_testing_left"
      )
      .collect()
      .map(_.toSeq.head.asInstanceOf[String])
      .map(MosaicGeometryJTS.fromHEX)
      .map(_.asInstanceOf[MosaicGeometryJTS].geom)

    val right2 = spark.sql(
        "select hex from format_testing_right"
      )
      .collect()
      .map(_.toSeq.head.asInstanceOf[String])
      .map(MosaicGeometryJTS.fromHEX)
      .map(_.asInstanceOf[MosaicGeometryJTS].geom)

    left2 should contain allElementsOf right2
  }

  test("Conversion from COORDS to GeoJSON") {
    mosaicContext.register(spark)
    val ss = spark
    import ss.implicits._

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

    val right: Array[String] = geojsonDf
      .select("geojson")
      .as[String]
      .collect()

    right.zip(left).foreach({ case (r: String, l: String) => r should matchJson(l)})

    coordsDf.createOrReplaceTempView("format_testing_left")
    geojsonDf.createOrReplaceTempView("format_testing_right")

    val left2: Array[String] = spark.sql(
        "select convert_to_geojson(coords)['json'] as geojson from format_testing_left"
      )
      .as[String]
      .collect()

    val right2: Array[String] = spark.sql(
        "select geojson from format_testing_right"
      )
      .as[String]
      .collect()

    right2.zip(left2).foreach({ case (r: String, l: String) => r should matchJson(l)})

    val left3 = coordsDf
      .select(
        st_asgeojson($"coords").getItem("json").alias("geojson")
      )
      .as[String]
      .collect()

    right.zip(left3).foreach({ case (r: String, l: String) => r should matchJson(l)})

  }

  test("Conversion from GeoJSON to WKB") {
    mosaicContext.register(spark)
    val ss = spark
    import ss.implicits._

    val geojsonDf: DataFrame = getGeoJSONDf
    val wkbDf: DataFrame = getHexRowsDf
      .select(as_hex($"hex").alias("hex"))
      .select(convert_to($"hex", "wkb").alias("wkb"))

    val left: Array[Array[Byte]] = geojsonDf
      .withColumn("geojson", as_json($"geojson"))
      .select(
        convert_to($"geojson", "wkb").alias("wkb")
      )
      .as[Array[Byte]]
      .collect()

    val right: Array[Array[Byte]] = wkbDf
      .as[Array[Byte]]
      .collect()

    left should contain theSameElementsAs right

    geojsonDf.createOrReplaceTempView("format_testing_left")
    wkbDf.createOrReplaceTempView("format_testing_right")

    val left2: Array[Array[Byte]] = spark.sql(
      "select convert_to_wkb(as_json(geojson)) as wkb from format_testing_left"
    )
      .as[Array[Byte]]
      .collect()
    val right2: Array[Array[Byte]] = spark.sql(
      "select wkb from format_testing_right"
    )
      .as[Array[Byte]]
      .collect()

    left2 should contain theSameElementsAs right2
  }

  test("Conversion from GeoJSON to WKT") {
    mosaicContext.register(spark)
    val ss = spark
    import ss.implicits._

    val geojsonDf: DataFrame = getGeoJSONDf.orderBy("id")
    val wktDf: DataFrame = getWKTRowsDf.orderBy("id")

    val left: Array[String] = geojsonDf
      .withColumn("geojson", as_json($"geojson"))
      .select(
        convert_to($"geojson", "wkt").alias("wkt")
      )
      .as[String]
      .collect()

    val right: Array[String] = wktDf
      .select("wkt")
      .as[String]
      .collect()

    left should contain theSameElementsAs right

    geojsonDf.createOrReplaceTempView("format_testing_left")
    wktDf.createOrReplaceTempView("format_testing_right")

    val left2: Array[String] = spark.sql(
        "select convert_to_wkt(as_json(geojson)) as wkt from format_testing_left"
      )
      .as[String]
      .collect()

    val right2: Array[String] = spark.sql(
        "select wkt from format_testing_right"
      )
      .as[String]
      .collect()

    left2 should contain theSameElementsAs right2
  }

  test("Conversion from GeoJSON to HEX") {
    mosaicContext.register(spark)
    val ss = spark
    import ss.implicits._

    val geojsonDf: DataFrame = getGeoJSONDf.orderBy("id")
    val hexDf: DataFrame = getHexRowsDf.orderBy("id")

    val left: Array[String] = geojsonDf
      .withColumn("geojson", as_json($"geojson"))
      .select(
        convert_to($"geojson", "hex").getItem("hex").alias("hex")
      )
      .as[String]
      .collect()

    val right: Array[String] = hexDf
      .select("hex")
      .as[String]
      .collect()

    left should contain theSameElementsAs right

    geojsonDf.createOrReplaceTempView("format_testing_left")
    hexDf.createOrReplaceTempView("format_testing_right")

    val left2: Array[String] = spark.sql(
        "select convert_to_hex(as_json(geojson))['hex'] as hex from format_testing_left"
      )
      .as[String]
      .collect()

    val right2: Array[String] = spark.sql(
        "select hex from format_testing_right"
      )
      .as[String]
      .collect()

    left2 should contain theSameElementsAs right2
  }

  test("Conversion from GeoJSON to COORDS") {
    mosaicContext.register(spark)
    val ss = spark
    import ss.implicits._

    val geojsonDf: DataFrame = getGeoJSONDf
    val coordsDf: DataFrame = getHexRowsDf
      .select(as_hex($"hex").alias("hex"))
      .select(convert_to($"hex", "coords").alias("coords"))

    val left: Array[Any] = geojsonDf
      .withColumn("geojson", as_json($"geojson"))
      .select(
        convert_to($"geojson", "coords").alias("coords")
      )
      .collect()
      .map(_.toSeq.head)

    val right: Array[Any] = coordsDf
      .collect()
      .map(_.toSeq.head)

    left should contain theSameElementsAs right

    geojsonDf.createOrReplaceTempView("format_testing_left")
    coordsDf.createOrReplaceTempView("format_testing_right")

    val left2: Array[Any] = spark.sql(
      "select convert_to_coords(as_json(geojson)) as coords from format_testing_left"
    )
      .collect()
      .map(_.toSeq.head)
    val right2: Array[Any] = spark.sql(
      "select coords from format_testing_right"
    )
      .collect()
      .map(_.toSeq.head)

    left2 should contain theSameElementsAs right2

    val left3 = geojsonDf.select(
        st_geomfromgeojson($"geojson").alias("coords")
      )
      .collect()
      .map(_.toSeq.head)

    left3 should contain allElementsOf right

    val left4 = spark.sql(
      "select st_geomfromgeojson(geojson) as coords from format_testing_left"
    )
      .collect()
      .map(_.toSeq.head)

    left4 should contain allElementsOf right
  }

}