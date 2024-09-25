package com.databricks.labs.mosaic.core.raster.api

import com.databricks.labs.mosaic.core.raster.gdal.{MosaicRasterBandGDAL, MosaicRasterGDAL}
import com.databricks.labs.mosaic.core.raster.io.RasterCleaner
import com.databricks.labs.mosaic.core.raster.operator.transform.RasterTransform
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import com.databricks.labs.mosaic.gdal.MosaicGDAL
import com.databricks.labs.mosaic.gdal.MosaicGDAL.configureGDAL
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants._

import java.util.UUID

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

    /** @return Returns whether using checkpoint (assumes `enable` called) */
    def isUseCheckpoint: Boolean = MosaicGDAL.isUseCheckpoint

    /** @return Returns checkpoint path (assumes `enable` called) */
    def getCheckpointPath: String = MosaicGDAL.getCheckpointPath

    /**
      * Enables GDAL on the worker nodes. GDAL requires drivers to be registered
      * on the worker nodes. This method registers all the drivers on the worker
      * nodes.
      * @param mosaicConfig
      *   The [[MosaicExpressionConfig]] for the op.
      */
    def enable(mosaicConfig: MosaicExpressionConfig): Unit = {
        configureGDAL(mosaicConfig)
        gdal.UseExceptions()
        gdal.AllRegister()
    }

    /**
      * Enables GDAL on the worker nodes. GDAL requires drivers to be registered
      * on the worker nodes. This method registers all the drivers on the worker
      * nodes.
      * @param spark
      *   Spark session from which to populate the [[MosaicExpressionConfig]].
      */
    def enable(spark: SparkSession): Unit = {
        val mosaicConfig = MosaicExpressionConfig(spark)
        enable(mosaicConfig)
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

    /**
     * Reads a raster from the given input data.
     * - If it is a byte array, it will read the raster from the byte array.
     * - If it is a string, it will read the raster from the path.
     * - Path may be a zip file.
     * - Path may be a subdataset.
     *
     * @param inputRaster
     *   The raster, based on inputDT. Path based rasters with subdatasets
     *   are supported.
     * @param createInfo
     *   Mosaic creation info of the raster. Note: This is not the same as the
     *   metadata of the raster. This is not the same as GDAL creation options.
     * @param inputDT
     *  [[DataType]] for the raster, either [[StringType]] or [[BinaryType]].
     * @return
     *   Returns a [[MosaicRasterGDAL]] object.
     */
    def readRaster(
                      inputRaster: Any,
                      createInfo: Map[String, String],
                      inputDT: DataType
                  ): MosaicRasterGDAL = {
        if (inputRaster == null) {
            MosaicRasterGDAL(null, createInfo)
        } else {
            inputDT match {
                case _: StringType =>
                    MosaicRasterGDAL.readRaster(createInfo)
                case _: BinaryType =>
                    val bytes = inputRaster.asInstanceOf[Array[Byte]]
                    try {
                        val rasterObj = MosaicRasterGDAL.readRaster(bytes, createInfo)
                        if (rasterObj.raster == null) {
                            val rasterZipObj = readParentZipBinary(bytes, createInfo)
                            if (rasterZipObj.raster == null) {
                                rasterObj // <- return initial
                            } else {
                                rasterZipObj
                            }
                        } else {
                            rasterObj
                        }
                    } catch {
                        case _: Throwable => readParentZipBinary(bytes, createInfo)
                    }
                case _ => throw new IllegalArgumentException(s"Unsupported data type: $inputDT")
            }
        }
    }

    private def readParentZipBinary(bytes: Array[Byte], createInfo: Map[String, String]): MosaicRasterGDAL = {
        try {
            val parentPath = createInfo("parentPath")
            val zippedPath = s"/vsizip/$parentPath"
            MosaicRasterGDAL.readRaster(bytes, createInfo + ("path" -> zippedPath))
        } catch {
            case _: Throwable => MosaicRasterGDAL(null, createInfo)
        }
    }

    /**
      * Writes the given rasters to either a path or a byte array.
      *
      * @param generatedRasters
      *   The rasters to write.
      * @param rasterDT
      *   The type of raster to write.
      *   - if string write to checkpoint
      *   - otherwise, write to bytes
      * @return
      *   Returns the paths of the written rasters.
      */
    def writeRasters(generatedRasters: Seq[MosaicRasterGDAL], rasterDT: DataType): Seq[Any] = {
        generatedRasters.map(raster =>
            if (raster != null) {
                rasterDT match {
                    case StringType =>
                        writeRasterString(raster)
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

    def writeRasterString(raster: MosaicRasterGDAL, path: Option[String] = None): UTF8String = {
        val uuid = UUID.randomUUID().toString
        val extension = GDAL.getExtension(raster.getDriversShortName)
        val writePath = path match {
            case Some(p) => p
            case None => s"${getCheckpointPath}/$uuid.$extension"
        }
        val outPath = raster.writeToPath(writePath)
        RasterCleaner.dispose(raster)
        UTF8String.fromString(outPath)
    }

    /**
      * Reads a raster from the given path. Assume not zipped file. If zipped,
      * use raster(path, vsizip = true)
      *
      * @param path
      *   The path to the raster. This path has to be a path to a single raster.
      *   Rasters with subdatasets are supported.
      * @param parentPath
      *   Parent path can help with detecting driver.
      * @return
      *   Returns a [[MosaicRasterGDAL]] object.
      */
    def raster(path: String, parentPath: String): MosaicRasterGDAL = {
        val createInfo = Map("path" -> path, "parentPath" -> parentPath)
        MosaicRasterGDAL.readRaster(createInfo)
    }

    /**
      * Reads a raster from the given byte array. If the byte array is a zip
      * file, it will read the raster from the zip file.
      *
      * @param content
      *   The byte array to read the raster from.
      * @param parentPath
      *   Parent path can help with detecting driver.
      * @param driverShortName
      *   Driver to use in reading.
      * @return
      *   Returns a [[MosaicRasterGDAL]] object.
      */
    def raster(content: Array[Byte], parentPath: String, driverShortName: String): MosaicRasterGDAL = {
        val createInfo = Map("parentPath" -> parentPath, "driver" -> driverShortName)
        MosaicRasterGDAL.readRaster(content, createInfo)
    }

    /**
      * Reads a raster from the given path. It extracts the specified band from
      * the raster. If zip, use band(path, bandIndex, vsizip = true)
      *
      * @param path
      *   The path to the raster. This path has to be a path to a single raster.
      *   Rasters with subdatasets are supported.
      * @param bandIndex
      *   The index of the band to read from the raster.
      * @param parentPath
      *   Parent path can help with detecting driver.
      * @return
      *   Returns a [[MosaicRasterBandGDAL]] object.
      */
    def band(path: String, bandIndex: Int, parentPath: String): MosaicRasterBandGDAL = {
        val createInfo = Map("path" -> path, "parentPath" -> parentPath)
        MosaicRasterGDAL.readBand(bandIndex, createInfo)
    }

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
