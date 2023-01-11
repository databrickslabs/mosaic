package com.databricks.labs.mosaic.core.raster

import org.gdal.gdal.Dataset

/**
  * A base API for managing raster data in Mosaic. Any raster abstraction should
  * extend this trait.
  *
  * @param path
  *   The path to the raster file. This has to be a path that can be read by the
  *   worker nodes.
  *
  * @param memSize
  *   The amount of memory occupied by the file in bytes.
  */
abstract class MosaicRaster(path: String, memSize: Long) extends Serializable {

    /** The path to the raster file. */
    def saveCheckpoint(id: Long, extent: (Int, Int, Int, Int), checkpointPath: String): String

    /** @return Returns the metadata of the raster file. */
    def metadata: Map[String, String]

    /**
      * @return
      *   Returns the key->value pairs of subdataset->description for the
      *   raster.
      */
    def subdatasets: Map[String, String]

    /** @return Returns the number of bands in the raster. */
    def numBands: Int

    /** @return Returns the SRID in the raster. */
    def SRID: Int

    /** @return Returns the proj4 projection string in the raster. */
    def proj4String: String

    /** @return Returns the x size of the raster. */
    def xSize: Int

    /** @return Returns the y size of the raster. */
    def ySize: Int

    /** @return Returns the bandId-th Band from the raster. */
    def getBand(bandId: Int): MosaicRasterBand

    /** @return Returns the extent(xmin, ymin, xmax, ymax) of the raster. */
    def extent: Seq[Double]

    /** @return Returns the GDAL Dataset representing the raster. */
    def getRaster: Dataset

    /** Cleans up the raster driver and references. */
    def cleanUp(): Unit

    /** @return Returns the amount of memory occupied by the file in bytes. */
    def getMemSize: Long = memSize

    /**
      * A template method for transforming the raster bands into new bands. Each
      * band is transformed into a new band using the transform function.
      * Override this method for tiling, clipping, warping, etc. type of
      * expressions.
      *
      * @tparam T
      *   The type of the result from the transformation of a band.
      * @param f
      *   The transform function. Will be applied on each band.
      */
    def transformBands[T](f: MosaicRasterBand => T): Seq[T]

}
