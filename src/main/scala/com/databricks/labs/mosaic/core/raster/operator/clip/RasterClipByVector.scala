package com.databricks.labs.mosaic.core.raster.operator.clip

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.raster.gdal.RasterGDAL
import com.databricks.labs.mosaic.core.raster.operator.gdal.GDALWarp
import com.databricks.labs.mosaic.functions.ExprConfig
import org.gdal.osr.SpatialReference

/**
  * RasterClipByVector is an object that defines the interface for clipping a
  * raster by a vector geometry.
  */
object RasterClipByVector {

    /**
      * Clips a raster by a vector geometry. The method handles all the
      * abstractions over GDAL Warp. By default it uses CUTLINE_ALL_TOUCHED=TRUE to ensure
      * that all pixels that touch the geometry are included. This will avoid
      * the issue of having a pixel that is half in and half out of the
      * geometry, important for tessellation. The method also uses the geometry
      * API to generate a shapefile that is used to clip the raster. The
      * shapefile is deleted after the clip is complete.
      *
      * @param raster
      *   The raster to clip.
      * @param geometry
      *   The geometry to clip by.
      * @param geomCRS
      *   The geometry CRS.
      * @param geometryAPI
      *   The geometry API.
      * @param exprConfigOpt
      *   Option [[ExprConfig]]
      * @param cutlineAllTouched
      *   Whether pixels touching cutline included (true)
      *   or only half-in (false), default: true.
      * @return
      *   A clipped raster.
      */
    def clip(
                raster: RasterGDAL, geometry: MosaicGeometry, geomCRS: SpatialReference,
                geometryAPI: GeometryAPI, exprConfigOpt: Option[ExprConfig], cutlineAllTouched: Boolean = true
            ): RasterGDAL = {
        val rasterCRS = raster.getSpatialReference
        val geomSrcCRS = if (geomCRS == null) rasterCRS else geomCRS
        val resultFileName = raster.createTmpFileFromDriver(exprConfigOpt)
        val shapePath = VectorClipper.generateClipper(geometry, geomSrcCRS, rasterCRS, geometryAPI, exprConfigOpt)

        // Reference https://gdal.org/programs/gdalwarp.html for cmd line usage
        // For more on -wo consult https://gdal.org/doxygen/structGDALWarpOptions.html
        // SOURCE_EXTRA=3 can also be used to ensure that when the raster is clipped, the
        // pixels that touch the geometry are included. The default is 1 for this, 3 might be a good empirical value.
        val cutlineToken: String = if (cutlineAllTouched) {
            " -wo CUTLINE_ALL_TOUCHED=TRUE"
        } else {
            ""
        }
        val cmd = s"gdalwarp$cutlineToken -cutline $shapePath -crop_to_cutline"

        /*
         * //scalastyle:off println
         * println(s"...clip command -> $cmd")
         * //scalastyle:on println
        */

        val result = GDALWarp.executeWarp(
            resultFileName,
            Seq(raster),
            command = cmd,
            exprConfigOpt
        )

        VectorClipper.cleanUpClipper(shapePath)

        result
    }

}
