package com.databricks.labs.mosaic.core.raster

/**
  * A base API for managing raster bands in Mosaic. Any raster band abstraction
  * should extend this trait.
  */
trait MosaicRasterBand extends Serializable {

    /** @return Returns the bandId of the band. */
    def index: Int

    /** @return Returns the description of the band. */
    def description: String

    /** @return Returns the metadata of the band. */
    def metadata: Map[String, String]

    /** @return Returns the unit type of the band pixels. */
    def units: String

    /** @return Returns the data type (numeric) of the band pixels. */
    def dataType: Int

    /** @return Returns the x size of the band. */
    def xSize: Int

    /** @return Returns the y size of the band. */
    def ySize: Int

    /** @return Returns the minimum pixel value of the band. */
    def minPixelValue: Double

    /** @return Returns the maximum pixel value of the band. */
    def maxPixelValue: Double

    /**
      * @return
      *   Returns the value used to represent transparent pixels of the band.
      */
    def noDataValue: Double

    /**
      * @return
      *   Returns the scale in which pixels are represented. It is the unit
      *   value of a pixel. If the pixel value is 5.1 and pixel scale is 10.0
      *   then the actual pixel value is 51.0.
      */
    def pixelValueScale: Double

    /**
      * @return
      *   Returns the offset in which pixels are represented. It is the unit
      *   value of a pixel. If the pixel value is 5.1 and pixel offset is 10.0
      *   then the actual pixel value is 15.1.
      */
    def pixelValueOffset: Double

    /**
      * @return
      *   Returns the pixel value with scale and offset applied. If the pixel
      *   value is 5.1 and pixel scale is 10.0 and pixel offset is 10.0 then the
      *   actual pixel value is 61.0.
      */
    def pixelValueToUnitValue(pixelValue: Double): Double

    /**
      * @return
      *   Returns the pixels of the raster as a 1D array.
      */
    def values: Array[Double] = values(0, 0, xSize, ySize)

    /**
     * @return
     *   Returns the pixels of the raster as a 1D array.
     */
    def maskValues: Array[Double] = maskValues(0, 0, xSize, ySize)

    /**
      * @param xOffset
      *   The x offset of the raster. The x offset is the number of pixels to
      *   skip from the left. 0 <= xOffset < xSize
      *
      * @param yOffset
      *   The y offset of the raster. The y offset is the number of pixels to
      *   skip from the top. 0 <= yOffset < ySize
      *
      * @param xSize
      *   The x size of the raster to be read.
      *
      * @param ySize
      *   The y size of the raster to be read.
      * @return
      *   Returns the pixels of the raster as a 1D array with offset and size
      *   applied.
      */
    def values(xOffset: Int, yOffset: Int, xSize: Int, ySize: Int): Array[Double]

    /**
     * @param xOffset
     *   The x offset of the raster. The x offset is the number of pixels to
     *   skip from the left. 0 <= xOffset < xSize
     *
     * @param yOffset
     *   The y offset of the raster. The y offset is the number of pixels to
     *   skip from the top. 0 <= yOffset < ySize
     *
     * @param xSize
     *   The x size of the raster to be read.
     *
     * @param ySize
     *   The y size of the raster to be read.
     * @return
     *   Returns the mask pixels of the raster as a 1D array with offset and size
     *   applied.
     */
    def maskValues(xOffset: Int, yOffset: Int, xSize: Int, ySize: Int): Array[Double]

    /**
      * Apply f to all pixels in the raster. Overridden in subclasses to define
      * the behavior.
      * @param f
      *   the function to apply to each pixel.
      * @param default
      *   the default value to use if the pixel is noData.
      * @tparam T
      *   the return type of the function.
      * @return
      *   an array of the results of applying f to each pixel.
      */
    def transformValues[T](f: (Int, Int, Double) => T, default: T = null): Seq[Seq[T]]

}
