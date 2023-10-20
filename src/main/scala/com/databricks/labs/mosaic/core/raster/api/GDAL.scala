package com.databricks.labs.mosaic.core.raster.api

import com.databricks.labs.mosaic.core.raster.gdal.{MosaicRasterBandGDAL, MosaicRasterGDAL}
import com.databricks.labs.mosaic.core.raster.io.RasterCleaner
import com.databricks.labs.mosaic.core.raster.operator.transform.RasterTransform
import com.databricks.labs.mosaic.gdal.MosaicGDAL.configureGDAL
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.gdal

/**
  * GDAL Raster API. It uses [[MosaicRasterGDAL]] as the
  * [[com.databricks.labs.mosaic.core.raster.io.RasterReader]].
  */
object GDAL {

    /** @return Returns the name of the raster API. */
    def name: String = "GDAL"

    /**
      * Enables GDAL on the worker nodes. GDAL requires drivers to be registered
      * on the worker nodes. This method registers all the drivers on the worker
      * nodes.
      */
    def enable(): Unit = {
        gdal.UseExceptions()
        configureGDAL()
        if (gdal.GetDriverCount() == 0) gdal.AllRegister()
    }

    def getExtension(driverShortName: String): String = {
        val driver = gdal.GetDriverByName(driverShortName)
        val result = driver.GetMetadataItem("DMD_EXTENSION")
        driver.delete()
        result
    }

    def readRaster(
        inputRaster: Any,
        parentPath: String,
        shortDriverName: String,
        inputDT: DataType
    ): MosaicRasterGDAL = {
        inputDT match {
            case StringType =>
                val path = inputRaster.asInstanceOf[UTF8String].toString
                MosaicRasterGDAL.readRaster(path, parentPath)
            case BinaryType =>
                val bytes = inputRaster.asInstanceOf[Array[Byte]]
                val raster = MosaicRasterGDAL.readRaster(bytes, parentPath, shortDriverName)
                // If the raster is coming as a byte array, we can't check for zip condition.
                // We first try to read the raster directly, if it fails, we read it as a zip.
                Option(raster).getOrElse(
                  MosaicRasterGDAL.readRaster(bytes, parentPath, shortDriverName)
                )
        }
    }

    def writeRasters(generatedRasters: Seq[MosaicRasterGDAL], checkpointPath: String, rasterDT: DataType): Seq[Any] = {
        generatedRasters.map(raster =>
            if (Option(raster).isDefined) {
                rasterDT match {
                    case StringType =>
                        val extension = GDAL.getExtension(raster.getDriversShortName)
                        val writePath = s"$checkpointPath/${raster.uuid}.$extension"
                        val outPath = raster.writeToPath(writePath)
                        RasterCleaner.dispose(raster)
                        UTF8String.fromString(outPath)
                    case BinaryType =>
                        val bytes = raster.writeToBytes()
                        RasterCleaner.dispose(raster)
                        bytes
                }
            } else {
                null
            }
        )
    }

    /**
      * Reads a raster from the given path. Assume not zipped file. If zipped,
      * use raster(path, vsizip = true)
      *
      * @param path
      *   The path to the raster. This path has to be a path to a single raster.
      *   Rasters with subdatasets are supported.
      * @return
      *   Returns a Raster object.
      */
    def raster(path: String, parentPath: String): MosaicRasterGDAL = MosaicRasterGDAL.readRaster(path, parentPath)

    def raster(content: Array[Byte], parentPath: String, driverShortName: String): MosaicRasterGDAL =
        MosaicRasterGDAL.readRaster(content, parentPath, driverShortName)

    /**
      * Reads a raster from the given path. It extracts the specified band from
      * the raster. If zip, use band(path, bandIndex, vsizip = true)
      *
      * @param path
      *   The path to the raster. This path has to be a path to a single raster.
      *   Rasters with subdatasets are supported.
      * @param bandIndex
      *   The index of the band to read from the raster.
      * @return
      *   Returns a Raster band object.
      */
    def band(path: String, bandIndex: Int, parentPath: String): MosaicRasterBandGDAL =
        MosaicRasterGDAL.readBand(path, bandIndex, parentPath)

    /**
      * Converts raster x, y coordinates to lat, lon coordinates.
      *
      * @param gt
      *   Geo transform of the raster.
      * @param x
      *   X coordinate of the raster.
      * @param y
      *   Y coordinate of the raster.
      * @return
      *   Returns a tuple of (lat, lon).
      */
    def toWorldCoord(gt: Seq[Double], x: Int, y: Int): (Double, Double) = {
        val (xGeo, yGeo) = RasterTransform.toWorldCoord(gt, x, y)
        (xGeo, yGeo)
    }

    /**
      * Converts lat, lon coordinates to raster x, y coordinates.
      *
      * @param gt
      *   Geo transform of the raster.
      * @param x
      *   Latitude of the raster.
      * @param y
      *   Longitude of the raster.
      * @return
      *   Returns a tuple of (xPixel, yPixel).
      */
    def fromWorldCoord(gt: Seq[Double], x: Double, y: Double): (Int, Int) = {
        val (xPixel, yPixel) = RasterTransform.fromWorldCoord(gt, x, y)
        (xPixel, yPixel)
    }

}
