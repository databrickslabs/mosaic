package com.databricks.labs.mosaic

import com.databricks.labs.mosaic.core.index.H3IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.test.SharedSparkSessionGDAL
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.{col, lit}
import org.gdal.ogr.ogr

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
    }

    test("multi read on geo_db") {
        val raster = "/binary/geodb/"
        val filePath = getClass.getResource(raster).getPath

        import com.databricks.labs.mosaic

        val df = mosaic.read
            .format("multi_read_ogr")
            .option("layerName", "Bridges_Feb2019")
            .option("vsizip", "true")
            .load(filePath)

        val mc = MosaicContext.build(H3, JTS)
        df.withColumn(
          "SHAPE_SRID",
          col("SHAPE").cast("int")
        ).withColumn(
          "projected",
          mc.functions.st_updatesrid(col("SHAPE"), col("SHAPE_SRID"), lit(4326))
        ).withColumn(
          "wkt",
          mc.functions.st_astext(col("projected"))
        ).show()
    }

}
