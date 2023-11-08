package com.databricks.labs.mosaic.core.raster.operator

import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.core.raster.operator.pixel.PixelCombineRasters

/** CombineAVG is a helper object for combining rasters using average. */
object CombineAVG {

    /**
      * Creates a new raster using average of input rasters. The average is
      * computed as (sum of all rasters) / (number of rasters). It is applied to
      * all bands of the input rasters. Please note the data type of the output
      * raster is double.
      *
      * @param rasters
      *   The rasters to compute result for.
      *
      * @return
      *   A new raster with average of input rasters.
      */
    def compute(rasters: => Seq[MosaicRasterGDAL]): MosaicRasterGDAL = {

        val pythonFunc = """
                           |import numpy as np
                           |import sys
                           |
                           |def average(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
                           |    stacked_array = np.array(in_ar)
                           |    pixel_sum = np.sum(stacked_array, axis=0)
                           |    div = np.sum(stacked_array > 0, axis=0)
                           |    div = np.where(div==0, 1, div)
                           |    np.divide(pixel_sum, div, out=out_ar, casting='unsafe')
                           |    np.clip(out_ar, stacked_array.min(), stacked_array.max(), out=out_ar)
                           |""".stripMargin
        PixelCombineRasters.combine(rasters, pythonFunc, "average")
    }

}
