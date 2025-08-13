package com.dblabs.spatial.rasterx.gdal.old

import org.apache.spark.util.SerializableConfiguration
import org.gdal.gdal.{Dataset, gdal}

import java.util.UUID

/**
  * RasterWriter is a trait that defines the interface for writing raster data
  * to a file system path or as bytes. It is used by the
  * [[com.databricks.labs.mosaic.core.raster.api.GDAL]] Raster API to write
  * rasters from the internal
  * [[com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL]] object.
  */
object RasterWriter {

    /**
      * Writes a raster to a byte array.
      *
      * @param destroy
      *   A boolean indicating if the raster should be destroyed after writing.
      * @return
      *   A byte array containing the raster data.
      */
    def writeToBytes(ds: Dataset, destroy: Boolean = true, hConf: SerializableConfiguration): Array[Byte] = {
        if (ds != null) {
            val driver = ds.GetDriver
            val driverName = driver.getShortName
            val extension = FormatLookup.formats(driverName)
            val uuid = UUID.randomUUID().toString.replace("-", "_")
            val outPath = s"/vsimem/$uuid.$extension"
            ds.FlushCache()
            driver.CreateCopy(outPath, ds, 1)
            val buffer = gdal.GetMemFileBuffer(outPath)
            buffer
        } else {
            Array.empty[Byte]
        }
    }

//    /**
//      * Writes a raster to a specified file system path.
//      *
//      * @param newPath
//      *   The path to write the raster.
//      * @param destroy
//      *   A boolean indicating if the raster should be destroyed after writing.
//      * @return
//      *   The path where written (may differ, e.g. due to subdatasets).
//      */
//    def writeToPath(newPath: String, destroy: Boolean = true, hConf: SerializableConfiguration): String = {
//
//    }

}
