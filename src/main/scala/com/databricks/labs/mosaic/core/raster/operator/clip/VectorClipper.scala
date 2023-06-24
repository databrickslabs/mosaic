package com.databricks.labs.mosaic.core.raster.operator.clip

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import org.gdal.ogr.ogrConstants.OFTInteger
import org.gdal.ogr.{DataSource, Feature, ogr}
import org.gdal.osr.SpatialReference

object VectorClipper {

    private def getShapefileName: String = {
        val uuid = java.util.UUID.randomUUID()
        val shapeFileName = s"/vsimem/${uuid.toString}.shp"
        shapeFileName
    }

    private def getShapefile(fileName: String): DataSource = {
        val shpDriver = ogr.GetDriverByName("ESRI Shapefile")
        val shpDataSource = shpDriver.CreateDataSource(fileName)
        shpDataSource
    }

    def generateClipper(geometry: MosaicGeometry, srcCrs: SpatialReference, dstCrs: SpatialReference, geometryAPI: GeometryAPI): String = {
        val shapeFileName = getShapefileName
        var shpDataSource = getShapefile(shapeFileName)

        val projectedGeom = geometry.osrTransformCRS(srcCrs, dstCrs, geometryAPI)

        val geom = ogr.CreateGeometryFromWkb(projectedGeom.toWKB)

        val geomLayer = shpDataSource.CreateLayer("geom")

        val idField = new org.gdal.ogr.FieldDefn("id", OFTInteger)
        geomLayer.CreateField(idField)
        val featureDefn = geomLayer.GetLayerDefn()
        val feature = new Feature(featureDefn)
        feature.SetGeometry(geom)
        feature.SetField("id", 1)
        geomLayer.CreateFeature(feature)

        shpDataSource.FlushCache()
        shpDataSource.delete()
        shpDataSource = null

        shapeFileName
    }

}
