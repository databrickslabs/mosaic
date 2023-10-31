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
                           |def average(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize,raster_ysize, buf_radius, gt, **kwargs):
                           |    div = np.zeros(in_ar[0].shape)
                           |    for i in range(len(in_ar)):
                           |        div += (in_ar[i] != 0)
                           |    div[div == 0] = 1
                           |
                           |    y = np.sum(in_ar, axis = 0, dtype = 'float64')
                           |    y = y / div
                           |
                           |    np.clip(y,0, sys.float_info.max, out = out_ar)
                           |""".stripMargin
        PixelCombineRasters.combine(rasters, pythonFunc, "average")
    }

}
