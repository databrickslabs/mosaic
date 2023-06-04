package com.databricks.labs.mosaic.core.raster.operator

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.core.raster.gdal_raster.MosaicRasterGDAL
import org.gdal.gdal.gdal

object RasterClipByVector {

    def clip(raster: MosaicRaster, geometry: MosaicGeometry, geometryAPI: GeometryAPI, reproject: Boolean): MosaicRaster = {
        val rasterCRS = raster.getRaster.GetSpatialRef()

        val uuid = java.util.UUID.randomUUID()
        val resultFileName = s"/vsimem/${raster.uuid}_${uuid.toString}.${raster.getExtension}"

        val shapeFileName = VectorClipper.generateClipper(geometry, geometryAPI, rasterCRS, reproject)

        val result = GDALWarp.executeWarp(
          resultFileName,
          raster,
          command = s"gdalwarp -cutline $shapeFileName -cwhere id='$uuid' -crop_to_cutline"
        )

        gdal.Unlink(shapeFileName)

        MosaicRasterGDAL(result, resultFileName)
    }

}
