package com.databricks.labs.mosaic.core.raster.operator

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.core.raster.gdal_raster.MosaicRasterGDAL
import com.databricks.labs.mosaic.utils.PathUtils

object NDVI {

    def emptyCopy(raster: MosaicRaster, path: String): MosaicRaster = {
        val driver = raster.getRaster.GetDriver()
        val newRaster = driver.Create(path, raster.xSize, raster.ySize, raster.numBands, raster.getRaster.GetRasterBand(1).getDataType)
        newRaster.SetGeoTransform(raster.getRaster.GetGeoTransform)
        newRaster.SetProjection(raster.getRaster.GetProjection)
        MosaicRasterGDAL(newRaster, path, isTemp = true)
    }

    def compute(raster: MosaicRaster, redIndex: Int, nirIndex: Int): MosaicRaster = {

        val redBand = raster.getRaster.GetRasterBand(redIndex)
        val nirBand = raster.getRaster.GetRasterBand(nirIndex)

        val numLines = redBand.GetYSize
        val lineSize = redBand.GetXSize

        val ndviPath = PathUtils.createTmpFilePath(raster.uuid.toString, raster.getExtension)
        val ndviRaster = emptyCopy(raster, ndviPath)

        var outputLine: Array[Double] = null
        var redScanline: Array[Double] = null
        var nirScanline: Array[Double] = null
        val dataType = org.gdal.gdalconst.gdalconstConstants.GDT_Float64
        for (line <- Range(0, numLines)) {
            redScanline = Array.fill[Double](lineSize)(0.0)
            nirScanline = Array.fill[Double](lineSize)(0.0)
            redBand.ReadRaster(0, line, lineSize, 1, dataType, redScanline)
            nirBand.ReadRaster(0, line, lineSize, 1, dataType, nirScanline)

            outputLine = redScanline.zip(nirScanline).map { case (red, nir) =>
                if (red + nir == 0) 0.0
                else (nir - red) / (red + nir)
            }
            ndviRaster.getRaster
                .GetRasterBand(1)
                .WriteRaster(0, line, lineSize, 1, dataType, outputLine.array)
        }
        outputLine = null
        redScanline = null
        nirScanline = null

        ndviRaster.flushCache()

        ndviRaster
    }

}
