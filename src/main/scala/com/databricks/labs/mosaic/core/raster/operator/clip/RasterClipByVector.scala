package com.databricks.labs.mosaic.core.raster.operator.clip

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.core.raster.operator.gdal.GDALWarp
import com.databricks.labs.mosaic.utils.PathUtils
import org.gdal.gdal.gdal
import org.gdal.osr.SpatialReference

object RasterClipByVector {

    def clip(raster: MosaicRaster, geometry: MosaicGeometry, geomCRS: SpatialReference, geometryAPI: GeometryAPI): MosaicRaster = {
        val rasterCRS = raster.getRaster.GetSpatialRef()
        val outShortName = raster.getRaster.GetDriver().getShortName

        val resultFileName = PathUtils.createTmpFilePath(raster.getExtension)

        val shapeFileName = VectorClipper.generateClipper(geometry, geomCRS, rasterCRS, geometryAPI)

        val result = GDALWarp.executeWarp(
          resultFileName,
          isTemp = true,
          Seq(raster),
          command = s"gdalwarp -of $outShortName -cutline $shapeFileName -crop_to_cutline -co COMPRESS=PACKBITS"
        )

        gdal.Unlink(shapeFileName)

        result
    }

}
