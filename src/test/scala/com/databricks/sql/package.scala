package com.databricks.sql

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

/**
  * Stub functions used for testing the reflection pattern for accessing
  * Databricks native H3 functions.
  */
//noinspection ScalaStyle
object functions {

    def sample_increment(i: Int): Int = {
        i + 1
    }

    def h3_longlatascellid(lon: Column, lat: Column, resolution: Column) = {
        lit("dummy_h3_longlatascellid")
    }

    def h3_polyfill(geom: Column, resolution: Column) = {
        lit("dummy_h3_polyfill")
    }

    def h3_boundaryaswkb(indexId: Column) = {
        lit("dummy_h3_boundaryaswkb")
    }

}
