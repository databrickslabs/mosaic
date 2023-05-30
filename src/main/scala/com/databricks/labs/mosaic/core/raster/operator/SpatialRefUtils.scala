package com.databricks.labs.mosaic.core.raster.operator

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.POLYGON
import org.gdal.osr.SpatialReference

object SpatialRefUtils {

    def getDestinationSR(raster: MosaicRaster, geometry: MosaicGeometry): SpatialReference = {
        val geomSR = new SpatialReference()
        geomSR.ImportFromEPSG(geometry.getSpatialReference)
        val rasterSR = raster.getRaster.GetSpatialRef()

        if (geomSR.IsSame(rasterSR) == 1) {
            rasterSR
        } else {
            geomSR
        }
    }

    def transform(geometry: MosaicGeometry, destSR: SpatialReference, geometryAPI: GeometryAPI): MosaicGeometry = {
        val geomSR = new SpatialReference()
        geomSR.ImportFromEPSG(geometry.getSpatialReference)
        // We need to swap the coordinates because GDAL is expecting (lat long)
        // and we are providing (long lat)
        geometryAPI.geometry(
            geometry.getShellPoints.head.map(p => geometryAPI.fromCoords(p.asSeq.reverse)),
            POLYGON
        ).osrTransformCRS(geomSR, destSR, geometryAPI)
    }

}
