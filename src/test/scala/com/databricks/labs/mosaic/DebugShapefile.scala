package com.databricks.labs.mosaic

import org.apache.spark.sql.test.SharedSparkSessionGDAL
import org.apache.spark.sql.QueryTest
import org.gdal.ogr.ogr
import org.scalatest.funsuite.AnyFunSuite

class DebugShapefile extends QueryTest with SharedSparkSessionGDAL {

    test("Debug Shapefile") {
        val shapefile = "/binary/shapefile/map.shp"
        val filePath = getClass.getResource(shapefile).getPath
        val dataset = ogr.GetDriverByName("ESRI Shapefile").Open(filePath, 0)
        dataset.GetLayer(0).GetFeature(0).GetFieldAsString("name")
    }

}
