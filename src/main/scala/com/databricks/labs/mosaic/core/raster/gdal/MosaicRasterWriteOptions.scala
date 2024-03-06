package com.databricks.labs.mosaic.core.raster.gdal

import com.databricks.labs.mosaic.gdal.MosaicGDAL
import org.gdal.osr.SpatialReference

case class MosaicRasterWriteOptions(
    compression: String = "DEFLATE",
    format: String = "GTiff",
    extension: String = "tif",
    resampling: String = "nearest",
    crs: SpatialReference = MosaicGDAL.WSG84, // Assume WGS84
    pixelSize: Option[(Double, Double)] = None,
    noDataValue: Option[Double] = None,
    missingGeoRef: Boolean = false,
    options: Map[String, String] = Map.empty[String, String]
)

object MosaicRasterWriteOptions {

    val VRT: MosaicRasterWriteOptions =
        MosaicRasterWriteOptions(
          compression = "NONE",
          format = "VRT",
          extension = "vrt",
          crs = MosaicGDAL.WSG84,
          pixelSize = None,
          noDataValue = None,
          options = Map.empty[String, String]
        )

    val GTiff: MosaicRasterWriteOptions = MosaicRasterWriteOptions()

    def noGPCsNoTransform(raster: MosaicRasterGDAL): Boolean = {
        val noGPCs = raster.getRaster.GetGCPCount == 0
        val noGeoTransform = raster.getRaster.GetGeoTransform == null ||
            (raster.getRaster.GetGeoTransform sameElements Array(0.0, 1.0, 0.0, 0.0, 0.0, 1.0))
        noGPCs && noGeoTransform
    }

    def apply(): MosaicRasterWriteOptions = new MosaicRasterWriteOptions()

    def apply(raster: MosaicRasterGDAL): MosaicRasterWriteOptions = {
        val compression = raster.getCompression
        val format = raster.getRaster.GetDriver.getShortName
        val extension = raster.getRasterFileExtension
        val resampling = "nearest"
        val pixelSize = None
        val noDataValue = None
        val options = Map.empty[String, String]
        val crs = raster.getSpatialReference
        val missingGeoRef = noGPCsNoTransform(raster)
        new MosaicRasterWriteOptions(compression, format, extension, resampling, crs, pixelSize, noDataValue, missingGeoRef, options)
    }

}
