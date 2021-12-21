package com.databricks.mosaic.expressions.geometry

import com.databricks.mosaic.functions.{convert_to, flatten_polygons, register}
import com.databricks.mosaic.mocks.getWKTRowsDf
import com.databricks.mosaic.test.SparkTest
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.locationtech.jts.io.{WKBReader, WKTReader}
import org.scalatest.{FunSuite, Matchers}

class TestFlattenPolygon extends FunSuite with SparkTest with Matchers {

  test("Flattening of WKB Polygons") {
    // we ensure that we have WKB test data by converting WKT testing data to WKB

    val spark = SparkSession.builder().getOrCreate()
    val df = getWKTRowsDf.withColumn("wkb", convert_to(col("wkt"), "wkb"))
    register(spark)

    val flattened = df.withColumn(
      "wkb", flatten_polygons(col("wkb"))
    ).select("wkb")

    val geoms = df.select("wkb")
      .collect()
      .map(g => new WKBReader().read(g.get(0).asInstanceOf[Array[Byte]]))
      .flatMap(g => for(i <- 0 until g.getNumGeometries) yield g.getGeometryN(i))

    val flattenedGeoms = flattened
      .collect()
      .map(g => new WKBReader().read(g.get(0).asInstanceOf[Array[Byte]]))

    flattenedGeoms should contain theSameElementsAs geoms
  }

  test("Flattening of WKT Polygons") {

    val spark = SparkSession.builder().getOrCreate()
    val df = getWKTRowsDf
    register(spark)

    val flattened = df.withColumn(
      "wkt", flatten_polygons(col("wkt"))
    ).select("wkt")

    val geoms = df.select("wkt")
      .collect()
      .map(g => new WKTReader().read(g.get(0).asInstanceOf[String]))
      .flatMap(g => for(i <- 0 until g.getNumGeometries) yield g.getGeometryN(i))

    val flattenedGeoms = flattened
      .collect()
      .map(g => new WKTReader().read(g.get(0).asInstanceOf[String]))

    flattenedGeoms should contain theSameElementsAs geoms
  }

  test("Flattening of Coords Polygons") {

    val spark = SparkSession.builder().getOrCreate()
    val df = getWKTRowsDf
      .withColumn("coords", convert_to(col("wkt"), "coords"))
    register(spark)

    val flattened = df.withColumn(
      "coords", flatten_polygons(col("coords"))
    ).select("coords")

    val geoms = df
      .withColumn("wkt", convert_to(col("coords"), "wkt"))
      .select("wkt")
      .collect()
      .map(g => new WKTReader().read(g.get(0).asInstanceOf[String]))
      .flatMap(g => for(i <- 0 until g.getNumGeometries) yield g.getGeometryN(i))

    val flattenedGeoms = flattened
      .withColumn("wkt", convert_to(col("coords"), "wkt"))
      .select("wkt")
      .collect()
      .map(g => new WKTReader().read(g.get(0).asInstanceOf[String]))

    flattenedGeoms should contain theSameElementsAs geoms
  }

  test("Flattening of Hex Polygons") {

    val spark = SparkSession.builder().getOrCreate()
    val df = getWKTRowsDf
      .withColumn("hex", convert_to(col("wkt"), "hex"))
    register(spark)

    val flattened = df.withColumn(
      "hex", flatten_polygons(col("hex"))
    ).select("hex")

    val geoms = df
      .withColumn("wkt", convert_to(col("hex"), "wkt"))
      .select("wkt")
      .collect()
      .map(g => new WKTReader().read(g.get(0).asInstanceOf[String]))
      .flatMap(g => for(i <- 0 until g.getNumGeometries) yield g.getGeometryN(i))

    val flattenedGeoms = flattened
      .withColumn("wkt", convert_to(col("hex"), "wkt"))
      .select("wkt")
      .collect()
      .map(g => new WKTReader().read(g.get(0).asInstanceOf[String]))

    flattenedGeoms should contain theSameElementsAs geoms
  }

}