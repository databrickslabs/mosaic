package com.databricks.labs.mosaic.core.raster.operator.gdal

import com.databricks.labs.mosaic.core.raster.gdal.{MosaicRasterGDAL, MosaicRasterWriteOptions}
import org.gdal.gdal.{RasterizeOptions, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.gdal.ogr.DataSource

import java.nio.file.{Files, Paths}


object GDALRasterize {

    def executeRasterize(outputPath: String, ds: DataSource, command: String): MosaicRasterGDAL = {
        require(command.startsWith("gdal_rasterize"), "Not a valid GDAL Rasterize command.")

        gdal.AllRegister()
        val writeOptions = MosaicRasterWriteOptions()

        val newRaster = gdal.GetDriverByName("GTiff").Create(outputPath, 5, 5, 1, gdalconstConstants.GDT_Float32)
        newRaster.SetSpatialRef(ds.GetLayer(0).GetSpatialRef())
//        newRaster.SetGeoTransform(Array(348000.0, 1.0, 0.0, 462000.0, 0.0, -1.0))
        newRaster.SetGeoTransform(Array(0.0, 1.0, 0.0, 5.0, 0.0, -1.0))
        val effectiveCommand = OperatorOptions.appendOptions(command, writeOptions)
        val rasterizeOptionsVec = OperatorOptions.parseOptions(effectiveCommand)
//        val rasterizeOptions = new RasterizeOptions(rasterizeOptionsVec)
        val bands = Array(1)
        val burnValues = Array(0.0)
        gdal.RasterizeLayer(newRaster, bands, ds.GetLayer(0), burnValues, rasterizeOptionsVec)
        newRaster.GetRasterBand(1).SetNoDataValue(-9999)
        newRaster.FlushCache()
        newRaster.delete()
        // Format will always be the same as the first raster
        val errorMsg = gdal.GetLastErrorMsg
        val size = Files.size(Paths.get(outputPath))
        val createInfo = Map(
            "path" -> outputPath,
            "parentPath" -> "", //rasters.head.getParentPath,
            "driver" -> writeOptions.format,
            "last_command" -> effectiveCommand,
            "last_error" -> errorMsg,
            "all_parents" -> ""
        )
        MosaicRasterGDAL(newRaster, createInfo, size)
    }

}
