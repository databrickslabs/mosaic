package com.databricks.labs.mosaic.core.raster.operator

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.utils.PathUtils

object NDVI {

    val doubleDataType: Int = org.gdal.gdalconst.gdalconstConstants.GDT_Float64

    def newNDVIRaster(raster: MosaicRasterGDAL, path: String): MosaicRasterGDAL = {
        val driver = raster.getRaster.GetDriver()
        // NDVI is always a single band raster with double data type
        val newRaster = driver.Create(path, raster.xSize, raster.ySize, 1, doubleDataType)
        newRaster.SetGeoTransform(raster.getRaster.GetGeoTransform)
        newRaster.SetProjection(raster.getRaster.GetProjection)
        // Avoid costly IO to compute MEM size here
        // It will be available when the raster is serialized for next operation
        // If value is needed then it will be computed when getMemSize is called
        MosaicRasterGDAL(newRaster, path, isTemp = true, raster.getParentPath, raster.getDriversShortName, -1)
    }

    def compute(raster: MosaicRasterGDAL, redIndex: Int, nirIndex: Int): MosaicRasterGDAL = {

        val redBand = raster.getRaster.GetRasterBand(redIndex)
        val nirBand = raster.getRaster.GetRasterBand(nirIndex)

        val numLines = redBand.GetYSize
        val lineSize = redBand.GetXSize

        val ndviPath = PathUtils.createTmpFilePath(raster.uuid.toString, GDAL.getExtension(raster.getDriversShortName))
        val ndviRaster = newNDVIRaster(raster, ndviPath)

        var outputLine: Array[Double] = null
        var redScanline: Array[Double] = null
        var nirScanline: Array[Double] = null

        for (line <- Range(0, numLines)) {
            redScanline = Array.fill[Double](lineSize)(0.0)
            nirScanline = Array.fill[Double](lineSize)(0.0)
            redBand.ReadRaster(0, line, lineSize, 1, doubleDataType, redScanline)
            nirBand.ReadRaster(0, line, lineSize, 1, doubleDataType, nirScanline)

            outputLine = redScanline.zip(nirScanline).map { case (red, nir) =>
                if (red + nir == 0) 0.0
                else (nir - red) / (red + nir)
            }
            ndviRaster.getRaster
                .GetRasterBand(1)
                .WriteRaster(0, line, lineSize, 1, doubleDataType, outputLine.array)
        }
        outputLine = null
        redScanline = null
        nirScanline = null

        ndviRaster.flushCache()

        ndviRaster
    }

}
