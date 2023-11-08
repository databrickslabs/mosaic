package com.databricks.labs.mosaic.core.raster.api

import com.databricks.labs.mosaic.core.raster.gdal.{MosaicRasterBandGDAL, MosaicRasterGDAL}
import com.databricks.labs.mosaic.core.raster.io.RasterCleaner
import com.databricks.labs.mosaic.core.raster.operator.transform.RasterTransform
import com.databricks.labs.mosaic.gdal.MosaicGDAL.configureGDAL
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants._

/**
  * GDAL Raster API. It uses [[MosaicRasterGDAL]] as the
  * [[com.databricks.labs.mosaic.core.raster.io.RasterReader]].
  */
object GDAL {

    /**
      * Returns the no data value for the given GDAL data type. For non-numeric
      * data types, it returns 0.0. For numeric data types, it returns the
      * minimum value of the data type. For unsigned data types, it returns the
      * maximum value of the data type.
      *
      * @param gdalType
      *   The GDAL data type.
      * @return
      *   Returns the no data value for the given GDAL data type.
      */
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

    /** @return Returns the name of the raster API. */
    def name: String = "GDAL"

    /**
      * Enables GDAL on the worker nodes. GDAL requires drivers to be registered
      * on the worker nodes. This method registers all the drivers on the worker
      * nodes.
      */
    def enable(): Unit = {
        configureGDAL()
        gdal.UseExceptions()
        gdal.AllRegister()
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
        driver.delete()
        result
    }

    /**
      * Reads a raster from the given input data. If it is a byte array, it will
      * read the raster from the byte array. If it is a string, it will read the
      * raster from the path. If the path is a zip file, it will read the raster
      * from the zip file. If the path is a subdataset, it will read the raster
      * from the subdataset.
      *
      * @param inputRaster
      *   The path to the raster. This path has to be a path to a single raster.
      *   Rasters with subdatasets are supported.
      * @return
      *   Returns a Raster object.
      */
    def readRaster(
        inputRaster: => Any,
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
                if (raster == null) {
                    val zippedPath = s"/vsizip/$parentPath"
                    MosaicRasterGDAL.readRaster(bytes, zippedPath, shortDriverName)
                } else {
                    raster
                }
        }
    }

    /**
      * Writes the given rasters to either a path or a byte array.
      *
      * @param generatedRasters
      *   The rasters to write.
      * @param checkpointPath
      *   The path to write the rasters to.
      * @return
      *   Returns the paths of the written rasters.
      */
    def writeRasters(generatedRasters: => Seq[MosaicRasterGDAL], checkpointPath: String, rasterDT: DataType): Seq[Any] = {
        generatedRasters.map(raster =>
            if (raster != null) {
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

    /**
      * Reads a raster from the given byte array. If the byte array is a zip
      * file, it will read the raster from the zip file.
      *
      * @param content
      *   The byte array to read the raster from.
      * @return
      *   Returns a Raster object.
      */
    def raster(content: => Array[Byte], parentPath: String, driverShortName: String): MosaicRasterGDAL =
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
