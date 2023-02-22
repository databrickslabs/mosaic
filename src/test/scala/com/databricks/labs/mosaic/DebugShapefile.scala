package com.databricks.labs.mosaic

import org.gdal.ogr.ogr
import org.scalatest.funsuite.AnyFunSuite

class DebugShapefile extends AnyFunSuite {

    test("Debug Shapefile") {
        val shapefile = "src/test/resources/shapefiles/shapefile.shp"
        val filePath = getClass.getResource(shapefile).getPath
        val dataset = ogr.GetDriverByName("ESRI Shapefile").Open(filePath, 0)
        dataset.GetLayer(0).GetFeature(0).GetFieldAsString("name")
    }

}
