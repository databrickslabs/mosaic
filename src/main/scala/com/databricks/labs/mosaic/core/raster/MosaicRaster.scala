package com.databricks.labs.mosaic.core.raster

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.raster.gdal_raster.RasterWriter
import org.gdal.gdal.Dataset
import org.gdal.osr.SpatialReference

/**
  * A base API for managing raster data in Mosaic. Any raster abstraction should
  * extend this trait. If the raster is in memory, then the path is the
  * "/vsimem/UUID.extension" of the raster. The in memory raster is not written
  * to disk but is kept in binary column. The default extension is ".tif" and
  * the format is COG (Cloud Optimized GeoTIFF).
  *
  * @param isInMem
  *   A flag to indicate if the raster is in memory or not.
  */
abstract class MosaicRaster(
    isInMem: Boolean
) extends Serializable with RasterWriter {

    def uuid: Long

    def getExtension: String

    def getPath: String


    def getRasterForCell(cellID: Long, indexSystem: IndexSystem, geometryAPI: GeometryAPI): MosaicRaster

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

    /** @return Returns the extent(xMin, yMin, xMax, yMax) of the raster. */
    def extent: Seq[Double]

    /**
      * @return
      *   Returns MosaicGeometry representing bounding box of the raster.
      */
    def bbox(geometryAPI: GeometryAPI, destCRS: SpatialReference): MosaicGeometry

    /** @return Returns the path to the raster file. */
    def getGeoTransform: Array[Double]

    /** @return Returns the GDAL Dataset representing the raster. */
    def getRaster: Dataset

    /** Cleans up the raster driver and references. */
    def cleanUp(): Unit

    /** @return Returns the amount of memory occupied by the file in bytes. */
    def getMemSize: Long

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

    /** A method that a boolean flat set to true if the raster is empty. */
    def isEmpty: Boolean

}
