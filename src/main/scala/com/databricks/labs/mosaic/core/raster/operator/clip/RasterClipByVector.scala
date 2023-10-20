package com.databricks.labs.mosaic.core.raster.operator.clip

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.core.raster.operator.gdal.GDALWarp
import com.databricks.labs.mosaic.utils.PathUtils
import org.gdal.gdal.gdal
import org.gdal.osr.SpatialReference

object RasterClipByVector {

    def clip(raster: MosaicRasterGDAL, geometry: MosaicGeometry, geomCRS: SpatialReference, geometryAPI: GeometryAPI): MosaicRasterGDAL = {
        val rasterCRS = raster.getRaster.GetSpatialRef()
        val outShortName = raster.getRaster.GetDriver().getShortName

        val resultFileName = PathUtils.createTmpFilePath(raster.uuid.toString, GDAL.getExtension(outShortName))

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
