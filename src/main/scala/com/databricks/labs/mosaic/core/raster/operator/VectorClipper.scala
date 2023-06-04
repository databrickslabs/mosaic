package com.databricks.labs.mosaic.core.raster.operator

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import org.gdal.ogr.ogrConstants.OFTInteger
import org.gdal.ogr.{Feature, ogr}
import org.gdal.osr.SpatialReference

object VectorClipper {

    def generateClipper(geometry: MosaicGeometry, geometryAPI: GeometryAPI, destCRS: SpatialReference, reproject: Boolean): String = {
        val geomCRS = new SpatialReference()
        // If SpatialReference is not set, then the EPSG code is 0, but 0 is not a valid EPSG code
        // so we need to check for that and set it to 4326.
        // OSR SpatialReference imported from 4326 is 0.
        val epsgCode = if (geometry.getSpatialReference == 0) 4326 else geometry.getSpatialReference
        geomCRS.ImportFromEPSG(epsgCode)

        val projectedGeom =
            if (reproject) {
                geometry.osrTransformCRS(geomCRS, destCRS, geometryAPI)
            } else {
                geometry
            }
        val geom = ogr.CreateGeometryFromWkb(projectedGeom.toWKB)

        val uuid = java.util.UUID.randomUUID()
        val shapeFileName = s"/vsimem/${uuid.toString}.shp"

        val shpDriver = ogr.GetDriverByName("ESRI Shapefile")
        val shpDataSource = shpDriver.CreateDataSource(shapeFileName)

        val geomLayer = shpDataSource.CreateLayer("geom", destCRS)

        val idField = new org.gdal.ogr.FieldDefn("id", OFTInteger)
        geomLayer.CreateField(idField)
        val featureDefn = geomLayer.GetLayerDefn()
        val feature = new Feature(featureDefn)
        feature.SetGeometry(geom)
        feature.SetField("id", uuid.toString)
        geomLayer.CreateFeature(feature)
        shpDataSource.FlushCache()

        shapeFileName
    }
}
