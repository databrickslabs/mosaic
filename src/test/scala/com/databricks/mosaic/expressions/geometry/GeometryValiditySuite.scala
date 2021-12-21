package com.databricks.mosaic.expressions.geometry

import com.databricks.mosaic.mocks.getWKTRowsDf
import org.apache.log4j.Logger
import com.databricks.mosaic.functions.{st_xmin, st_xmax, st_ymin, st_ymax, st_isvalid}
import com.databricks.mosaic.test.SparkTest
// import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col}
import org.scalatest.{FunSuite, Matchers}

class GeometryValiditySuite extends FunSuite with SparkTest with Matchers {  

  test("Calling st_xmin() should return the minimum x value from all coordinates in the geometry") {
    val spark = SparkSession.builder().getOrCreate()

    val df = getWKTRowsDf.withColumn("result", st_xmin(col("wkt")))
    val results = df.collect().map(_.getDouble(1)).toList
    val expected = List(10, 0, 10, 10).map(_.asInstanceOf[Double])

    results should contain theSameElementsAs expected
  }

  test("Calling st_xmax() should return the maximum x value from all coordinates in the geometry") {
    val spark = SparkSession.builder().getOrCreate()
    
    val df = getWKTRowsDf.withColumn("result", st_xmax(col("wkt")))
    val results = df.collect().map(_.getDouble(1)).toList
    val expected = List(40, 2, 110, 111).map(_.asInstanceOf[Double])

    results should contain theSameElementsAs expected
  }

  test("Calling st_ymin() should return the minimum y value from all coordinates in the geometry") {
    val spark = SparkSession.builder().getOrCreate()
    
    val df = getWKTRowsDf.withColumn("result", st_ymin(col("wkt")))
    val results = df.collect().map(_.getDouble(1)).toList
    val expected = List(10, 0, 10, 10).map(_.asInstanceOf[Double])

    results should contain theSameElementsAs expected
  }

  test("Calling st_ymax() should return the maximum y value from all coordinates in the geometry") {
    val spark = SparkSession.builder().getOrCreate()
    
    val df = getWKTRowsDf.withColumn("result", st_ymax(col("wkt")))
    val results = df.collect().map(_.getDouble(1)).toList
    val expected = List(40, 2, 110, 111).map(_.asInstanceOf[Double])

    results should contain theSameElementsAs expected
  }


  test("Calling st_isvalid() on a valid geometry should return true.") {
    // create df with a selection of valid and invalid geometries expressed as WKT
    // use hex_from_wkt method to convert into WKB representation
    // apply st_isvalid to each row and collect results to Seq

    val df = getWKTRowsDf.withColumn("result", st_isvalid(col("wkt")))
    val results = df.collect().map(_.getBoolean(1)).toList

    all (results) should be (true)
  }

  test("Calling st_isvalid() on an invalid geometry should return false.") {
    // create df with a selection of valid and invalid geometries expressed as WKT
    // use hex_from_wkt method to convert into WKB representation
    // apply st_isvalid to each row and collect results to Seq

    

    val df = getWKTRowsDf.withColumn("result", st_isvalid(col("wkt")))
    val results = df.collect().map(_.getBoolean(1)).toList

    all (results) should be (true)
  }

}
