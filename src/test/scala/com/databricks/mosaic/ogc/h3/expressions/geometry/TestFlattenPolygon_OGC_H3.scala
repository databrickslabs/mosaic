package com.databricks.mosaic.ogc.h3.expressions.geometry

import org.locationtech.jts.io.{WKBReader, WKTReader}
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.functions.col

import com.databricks.mosaic.core.geometry.api.GeometryAPI.OGC
import com.databricks.mosaic.core.index.H3IndexSystem
import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.mocks.getWKTRowsDf
import com.databricks.mosaic.test.SparkFlatSpec

class TestFlattenPolygon_OGC_H3 extends SparkFlatSpec with Matchers {

    val mosaicContext: MosaicContext = MosaicContext.build(H3IndexSystem, OGC)

    import mosaicContext.functions._

    it should "Flattening of WKB Polygons" in {
        mosaicContext.register(spark)
        // we ensure that we have WKB test data by converting WKT testing data to WKB

        val df = getWKTRowsDf.withColumn("wkb", convert_to(col("wkt"), "wkb"))

        val flattened = df
            .withColumn(
              "wkb",
              flatten_polygons(col("wkb"))
            )
            .select("wkb")

        val geoms = df
            .select("wkb")
            .collect()
            .map(g => new WKBReader().read(g.get(0).asInstanceOf[Array[Byte]]))
            .flatMap(g => for (i <- 0 until g.getNumGeometries) yield g.getGeometryN(i))

        val flattenedGeoms = flattened
            .collect()
            .map(g => new WKBReader().read(g.get(0).asInstanceOf[Array[Byte]]))

        flattenedGeoms should contain theSameElementsAs geoms
    }

    it should "Flattening of WKT Polygons" in {
        mosaicContext.register(spark)

        val df = getWKTRowsDf

        val flattened = df
            .withColumn(
              "wkt",
              flatten_polygons(col("wkt"))
            )
            .select("wkt")

        val geoms = df
            .select("wkt")
            .collect()
            .map(g => new WKTReader().read(g.get(0).asInstanceOf[String]))
            .flatMap(g => for (i <- 0 until g.getNumGeometries) yield g.getGeometryN(i))
            .map(_.getCentroid) // proxy to avoid clockwise vs anti-clockwise

        val flattenedGeoms = flattened
            .collect()
            .map(g => new WKTReader().read(g.get(0).asInstanceOf[String]))
            .map(_.getCentroid)

        flattenedGeoms should contain theSameElementsAs geoms
    }

    it should "Flattening of Coords Polygons" in {
        mosaicContext.register(spark)

        val df = getWKTRowsDf
            .withColumn("coords", convert_to(col("wkt"), "coords"))

        val flattened = df
            .withColumn(
              "coords",
              flatten_polygons(col("coords"))
            )
            .select("coords")

        val geoms = df
            .withColumn("wkt", convert_to(col("coords"), "wkt"))
            .select("wkt")
            .collect()
            .map(g => new WKTReader().read(g.get(0).asInstanceOf[String]))
            .flatMap(g => for (i <- 0 until g.getNumGeometries) yield g.getGeometryN(i))
            .map(_.getCentroid) // proxy to avoid clockwise vs anti-clockwise

        val flattenedGeoms = flattened
            .withColumn("wkt", convert_to(col("coords"), "wkt"))
            .select("wkt")
            .collect()
            .map(g => new WKTReader().read(g.get(0).asInstanceOf[String]))
            .map(_.getCentroid)

        flattenedGeoms should contain theSameElementsAs geoms
    }

    it should "Flattening of Hex Polygons" in {
        mosaicContext.register(spark)

        val df = getWKTRowsDf
            .withColumn("hex", convert_to(col("wkt"), "hex"))

        val flattened = df
            .withColumn(
              "hex",
              flatten_polygons(col("hex"))
            )
            .select("hex")

        val geoms = df
            .withColumn("wkt", convert_to(col("hex"), "wkt"))
            .select("wkt")
            .collect()
            .map(g => new WKTReader().read(g.get(0).asInstanceOf[String]))
            .flatMap(g => for (i <- 0 until g.getNumGeometries) yield g.getGeometryN(i))

        val flattenedGeoms = flattened
            .withColumn("wkt", convert_to(col("hex"), "wkt"))
            .select("wkt")
            .collect()
            .map(g => new WKTReader().read(g.get(0).asInstanceOf[String]))

        flattenedGeoms should contain theSameElementsAs geoms
    }

}
