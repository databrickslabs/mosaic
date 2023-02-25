package com.databricks.labs.mosaic

import com.databricks.labs.mosaic.core.index.H3IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.test.SharedSparkSessionGDAL
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.col
import org.gdal.ogr.ogr
import org.scalatest.funsuite.AnyFunSuite

class DebugShapefile extends QueryTest with SharedSparkSessionGDAL {

    test("Debug Shapefile") {
        val shapefile = "/binary/shapefile/map.shp"
        val filePath = getClass.getResource(shapefile).getPath
        val dataset = ogr.GetDriverByName("ESRI Shapefile").Open(filePath, 0)
        dataset.GetLayer(0).GetFeature(0).GetFieldAsString("name")
    }

    test("shapefile testing") {
        val mc = MosaicContext.build(H3IndexSystem, JTS)
        val shapefile = "/binary/shapefile/map.shp"
        val filePath = getClass.getResource(shapefile).getPath
        val df = spark.read.format("ogr").option("driverName", "ESRI Shapefile").load(filePath)
        df.show()
        val df2 = spark.read.format("shapefile").load(filePath)
        df2.withColumn("x", mc.functions.st_astext(col("geom_0"))).show()
        df2.printSchema()
    }

    test("raster reading") {
        val rasters = "/binary/grib-cams"
        val path = getClass.getResource(rasters).getPath
        val df = spark.read.format("gdal").option("driverName", "GRIB").load(path)
        df.show()
    }

    test("geodb reading") {
        val rasters = "/binary/geodb"
        val path = getClass.getResource(rasters).getPath
        val df = spark.read.format("ogr").load(path)
        df.show()
        df.printSchema()
        println(df.count())
    }

}
