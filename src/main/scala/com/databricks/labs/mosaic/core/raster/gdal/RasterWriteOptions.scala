package com.databricks.labs.mosaic.core.raster.gdal

import com.databricks.labs.mosaic.core.raster.io.RasterIO.identifyExtFromDriver
import com.databricks.labs.mosaic.gdal.MosaicGDAL
import org.gdal.osr.SpatialReference

import scala.util.Try

case class RasterWriteOptions(
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

object RasterWriteOptions {

    val VRT: RasterWriteOptions =
        RasterWriteOptions(
          compression = "NONE",
          format = "VRT",
          extension = "vrt",
          crs = MosaicGDAL.WSG84,
          pixelSize = None,
          noDataValue = None,
          options = Map.empty[String, String]
        )

    val GTiff: RasterWriteOptions = RasterWriteOptions()

    def noGPCsNoTransform(raster: RasterGDAL): Boolean = Try {
        val dataset = raster.withDatasetHydratedOpt().get
        val noGPCs = dataset.GetGCPCount == 0
        val noGeoTransform = dataset.GetGeoTransform() == null ||
        (dataset.GetGeoTransform() sameElements Array (0.0, 1.0, 0.0, 0.0, 0.0, 1.0) )
        noGPCs && noGeoTransform
    }.getOrElse(true)

    def apply(): RasterWriteOptions = new RasterWriteOptions()

    def apply(raster: RasterGDAL): RasterWriteOptions = {
        val compression = raster.getCompression
        val driverShortName = raster.getDriverName() // driver
        val extension = identifyExtFromDriver(driverShortName)
        val resampling = "nearest"
        val pixelSize = None
        val noDataValue = None
        val options = Map.empty[String, String]
        val crs = raster.getSpatialReference
        val missingGeoRef = noGPCsNoTransform(raster)
        new RasterWriteOptions(
            compression,
            format = driverShortName,
            extension,
            resampling,
            crs,
            pixelSize,
            noDataValue,
            missingGeoRef,
            options
        )
    }

}
