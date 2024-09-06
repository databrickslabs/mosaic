package com.databricks.labs.mosaic.core.raster.operator.clip

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.raster.gdal.RasterGDAL
import com.databricks.labs.mosaic.core.raster.operator.gdal.GDALWarp
import com.databricks.labs.mosaic.functions.ExprConfig
import org.gdal.osr.SpatialReference

/**
  * RasterClipByVector is an object that defines the interface for clipping a
  * tile by a vector geometry.
  */
object RasterClipByVector {

    /**
      * Clips a tile by a vector geometry. The method handles all the
      * abstractions over GDAL Warp. By default it uses CUTLINE_ALL_TOUCHED=TRUE to ensure
      * that all pixels that touch the geometry are included. This will avoid
      * the issue of having a pixel that is half in and half out of the
      * geometry, important for tessellation. The method also uses the geometry
      * API to generate a shapefile that is used to clip the tile. The
      * shapefile is deleted after the clip is complete.
      *
      * @param raster
      *   The tile to clip.
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
      * @param skipProject
      *   Whether to ignore Spatial Reference on source (as-provided data); this is useful
      *   for data that does not have SRS but nonetheless conforms to index, (default is false).
      * @return
      *   A clipped tile.
      */
    def clip(
                raster: RasterGDAL, geometry: MosaicGeometry, geomCRS: SpatialReference,
                geometryAPI: GeometryAPI, exprConfigOpt: Option[ExprConfig],
                cutlineAllTouched: Boolean = true, skipProject: Boolean = false
            ): RasterGDAL = {

        val rasterSRS =
            if (!skipProject) raster.getSpatialReference // <- this will default to WGS84
            else geomCRS
        val geomSRS = if (geomCRS == null) rasterSRS else geomCRS
        val resultFileName = raster.createTmpFileFromDriver(exprConfigOpt)
        val shapePath = VectorClipper.generateClipper(
            geometry,
            geomSRS,
            rasterSRS,
            geometryAPI,
            exprConfigOpt
        )

        // Reference https://gdal.org/programs/gdalwarp.html for cmd line usage
        // For more on -wo consult https://gdal.org/doxygen/structGDALWarpOptions.html
        // SOURCE_EXTRA=3 can also be used to ensure that when the tile is clipped, the
        // pixels that touch the geometry are included. The default is 1 for this, 3 might be a good empirical value.
        val cutlineToken: String =
            if (cutlineAllTouched) " -wo CUTLINE_ALL_TOUCHED=TRUE"
            else ""

        //https://gdal.org/programs/gdalwarp.html#cmdoption-gdalwarp-s_srs
        val srsToken: String =
            if (!skipProject && geomSRS.IsSame(rasterSRS) != 1) "" // <- '1' means equivalent SRS
            else {
                // SRS treated as equivalent (use geomSRS)
                // Note that null is the right value in these api calls
                val authToken = s"${geomSRS.GetAuthorityName(null)}:${geomSRS.GetAuthorityCode(null)}"
                s" -s_srs $authToken -t_srs $authToken"
            }
        val cmd = s"gdalwarp${cutlineToken} -cutline ${shapePath} -crop_to_cutline${srsToken}"
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
