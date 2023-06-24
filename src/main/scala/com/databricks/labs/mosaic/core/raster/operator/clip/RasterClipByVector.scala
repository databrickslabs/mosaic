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

        val uuid = java.util.UUID.randomUUID()
        //val resultFileName = s"/vsimem/${raster.uuid}_${uuid.toString}.${raster.getExtension}"
        val resultFileName = PathUtils.createTmpFilePath(raster.uuid.toString, raster.getExtension)

        val shapeFileName = VectorClipper.generateClipper(geometry, geomCRS, rasterCRS, geometryAPI)

        val result = GDALWarp.executeWarp(
          resultFileName,
          Seq(raster),
          command = s"gdalwarp -of $outShortName -cutline $shapeFileName -crop_to_cutline -multi -wm 500 -wo NUM_THREADS=ALL_CPUS -co NUM_THREADS=ALL_CPUS"
        )

        gdal.Unlink(shapeFileName)

        result
    }

}
