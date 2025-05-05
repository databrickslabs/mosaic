package com.databricks.labs.mosaic.core.raster.operator.clip

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.utils.PathUtils
import org.gdal.gdal.gdal
import org.gdal.ogr.ogrConstants.OFTInteger
import org.gdal.ogr.{DataSource, Feature, ogr}
import org.gdal.osr.SpatialReference

import scala.util.Try

/**
  * VectorClipper is an object that defines the interface for managing a clipper
  * shapefile used for clipping a raster by a vector geometry.
  */
object VectorClipper {

    /**
      * Generates an in memory shapefile that is used to clip a raster.
      * @return
      *   The shapefile name.
      */
    private def getShapefileName: String = {
        val shapeFileName = PathUtils.createTmpFilePath("shp")
        shapeFileName
    }

    /**
      * Generates a shapefile data source that is used to clip a raster.
      * @param fileName
      *   The shapefile data source.
      * @return
      *   The shapefile.
      */
    private def getShapefile(fileName: String): DataSource = {
        val shpDriver = ogr.GetDriverByName("ESRI Shapefile")
        val shpDataSource = shpDriver.CreateDataSource(fileName)
        shpDataSource
    }

    /**
      * Generates a clipper shapefile that is used to clip a raster. The
      * shapefile is flushed to disk and then the data source is deleted. The
      * shapefile is accessed by gdalwarp by file name.
      * @note
      *   The shapefile is generated in memory.
      *
      * @param geometry
      *   The geometry to clip by.
      * @param srcCrs
      *   The geometry CRS.
      * @param dstCrs
      *   The raster CRS.
      * @param geometryAPI
      *   The geometry API.
      * @return
      *   The shapefile name.
      */
    def generateClipper(geometry: MosaicGeometry, geomCRS: SpatialReference, raster: MosaicRasterGDAL, geometryAPI: GeometryAPI): String = {
        val rasterCRS = raster.getSpatialReference
        val shapeFileName = getShapefileName
        var shpDataSource = getShapefile(shapeFileName)
        val geomSrcCRS = if (geomCRS == null) rasterCRS else geomCRS

        val projectedGeom = geometry.osrTransformCRS(geomSrcCRS, rasterCRS, geometryAPI)

        val factor = 0.5 * raster.pixelDiagSize
        val pixelArea = Math.abs(raster.pixelXSize * raster.pixelYSize)
        val adjustedGeom = if (projectedGeom.getArea < pixelArea) projectedGeom.buffer(factor) else projectedGeom

        val geom = ogr.CreateGeometryFromWkb(adjustedGeom.toWKB(2))
        geom.AssignSpatialReference(rasterCRS)

        val geomLayer = shpDataSource.CreateLayer("geom", rasterCRS)

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

    /**
      * Cleans up the clipper shapefile.
      *
      * @param shapeFileName
      *   The shapefile to clean up.
      */
    def cleanUpClipper(shapeFileName: String): Unit = {
        Try(ogr.GetDriverByName("ESRI Shapefile").DeleteDataSource(shapeFileName))
        Try(gdal.Unlink(shapeFileName))
    }

}
