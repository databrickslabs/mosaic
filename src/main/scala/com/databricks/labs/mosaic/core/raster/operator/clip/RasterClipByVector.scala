package com.databricks.labs.mosaic.core.raster.operator.clip

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.core.raster.operator.gdal.GDALWarp
import com.databricks.labs.mosaic.utils.PathUtils
import org.gdal.osr.SpatialReference

import java.util.Locale

/**
  * RasterClipByVector is an object that defines the interface for clipping a
  * raster by a vector geometry.
  */
object RasterClipByVector {

    /**
      * Clips a raster by a vector geometry. The method handles all the
      * abstractions over GDAL Warp. It uses CUTLINE_ALL_TOUCHED=TRUE to ensure
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
      * @param cutlineAllTouched
      *   Whether pixels touching cutline are automatically included (true) or
      *   included only the cutline crosses the centre point of the pixel
      *   (false) default: true.
      * @return
      *   A clipped raster.
      */
    def clip(
        raster: MosaicRasterGDAL,
        geometry: MosaicGeometry,
        geomCRS: SpatialReference,
        geometryAPI: GeometryAPI,
        cutlineAllTouched: Boolean = true
    ): MosaicRasterGDAL = {
        val rasterCRS = raster.getSpatialReference
        val outShortName = raster.getDriversShortName
        val geomSrcCRS = if (geomCRS == null) rasterCRS else geomCRS

        val cutlineToken = cutlineAllTouched.toString.toUpperCase(Locale.US)

        val resultFileName = PathUtils.createTmpFilePath(GDAL.getExtension(outShortName))

        val shapeFileName = VectorClipper.generateClipper(geometry, geomSrcCRS, rasterCRS, geometryAPI)

        // For -wo consult https://gdal.org/doxygen/structGDALWarpOptions.html
        val result = GDALWarp.executeWarp(
          resultFileName,
          Seq(raster),
          command = s"gdalwarp -wo CUTLINE_ALL_TOUCHED=$cutlineToken -cutline $shapeFileName -crop_to_cutline"
        )

        VectorClipper.cleanUpClipper(shapeFileName)
        PathUtils.cleanUpPath(shapeFileName)

        result
    }

}
