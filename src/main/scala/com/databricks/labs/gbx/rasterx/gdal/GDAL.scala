package com.databricks.labs.gbx.rasterx.gdal

import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants._
import org.gdal.osr.SpatialReference

object GDAL {

    val WSG84: SpatialReference = {
        val wsg84 = new SpatialReference()
        wsg84.ImportFromEPSG(4326)
        wsg84.SetAxisMappingStrategy(org.gdal.osr.osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
        wsg84
    }

    val EPSG3857: SpatialReference = {
        val epsg3857 = new SpatialReference()
        epsg3857.ImportFromEPSG(3857)
        epsg3857.SetAxisMappingStrategy(org.gdal.osr.osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
        epsg3857
    }

    /**
      * Returns the extension of the given driver.
      * @param driverShortName
      *   The short name of the driver. For example, GTiff.
      * @return
      *   Returns the extension of the driver. For example, tif.
      */
    def getExtension(driverShortName: String): String = {
        val driver = gdal.GetDriverByName(driverShortName)
        val result = driver.GetMetadataItem("DMD_EXTENSION")
        val toReturn = if (result == null) FormatLookup.formats(driverShortName) else result
        driver.delete()
        toReturn
    }

    def getNoDataConstant(gdalType: Int): Double = {
        gdalType match {
            case GDT_Unknown => 0.0
            case GDT_Byte    => 0.0
            // Unsigned Int16 is Char in scala
            // https://www.tutorialspoint.com/scala/scala_data_types.htm
            case GDT_UInt16  => Char.MaxValue.toDouble
            case GDT_Int16   => Short.MinValue.toDouble
            case GDT_UInt32  => 2 * Int.MaxValue.toDouble
            case GDT_Int32   => Int.MinValue.toDouble
            case GDT_Float32 => Float.MinValue.toDouble
            case GDT_Float64 => Double.MinValue
            case _           => 0.0
        }
    }

    def toWorldCoord(gt: Array[Double], x: Int, y: Int): (Double, Double) = {
        val offset = 0.5 // offset to center of the pixel
        val xGeo = gt(0) + (x + offset) * gt(1) + (y + offset) * gt(2)
        val yGeo = gt(3) + (x + offset) * gt(4) + (y + offset) * gt(5)
        (xGeo, yGeo)
    }

    def fromWorldCoord(gt: Array[Double], xGeo: Double, yGeo: Double): (Int, Int) = {
        val det = gt(1) * gt(5) - gt(2) * gt(4)
        val dx = xGeo - gt(0)
        val dy = yGeo - gt(3)
        val x = ( dx * gt(5) - dy * gt(2)) / det
        val y = (-dx * gt(4) + dy * gt(1)) / det
        (x.toInt, y.toInt)
    }

}
