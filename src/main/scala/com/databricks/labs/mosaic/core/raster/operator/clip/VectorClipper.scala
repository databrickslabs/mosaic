package com.databricks.labs.mosaic.core.raster.operator.clip

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.raster.io.RasterIO.flushAndDestroy
import com.databricks.labs.mosaic.functions.ExprConfig
import com.databricks.labs.mosaic.utils.PathUtils
import org.gdal.gdal.gdal
import org.gdal.ogr.ogrConstants.OFTInteger
import org.gdal.ogr.{DataSource, Feature, ogr}
import org.gdal.osr.SpatialReference

import scala.util.Try

/**
  * VectorClipper is an object that defines the interface for managing a clipper
  * shapefile used for clipping a tile by a vector geometry.
  */
object VectorClipper {

    /**
      * Generates an in memory shapefile that is used to clip a tile.
      * @param exprConfigOpt
      *   Option [[ExprConfig]]
      * @return
      *   The shapefile name.
      */
    private def getShapefilePath(exprConfigOpt: Option[ExprConfig]): String = {
        val shapeFileName = PathUtils.createTmpFilePath("shp", exprConfigOpt)
        shapeFileName
    }

    /**
      * Generates a shapefile data source that is used to clip a tile.
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
      * Generates a clipper shapefile that is used to clip a tile. The
      * shapefile is flushed to disk and then the data source is deleted. The
      * shapefile is accessed by gdalwarp by file name.
     *
     * @note
      *   The shapefile is generated in memory.
     * @param geometry
      *   The geometry to clip by.
      * @param srcCrs
      *   The geometry CRS.
      * @param dstCrs
      *   The tile CRS.
      * @param geometryAPI
      *   The geometry API.
      * @param exprConfigOpt
      *   Option [[ExprConfig]]
      * @return
      *   The shapefile name.
      */
    def generateClipper(
                           geometry: MosaicGeometry,
                           srcCrs: SpatialReference,
                           dstCrs: SpatialReference,
                           geometryAPI: GeometryAPI,
                           exprConfigOpt: Option[ExprConfig]
                       ): String = {
        val shapePath = getShapefilePath(exprConfigOpt)
        val shpDataSource: DataSource = getShapefile(shapePath) // note: not a Dataset
        val projectedGeom = geometry.osrTransformCRS(srcCrs, dstCrs, geometryAPI)
        val geom = ogr.CreateGeometryFromWkb(projectedGeom.toWKB)

        // 0.4.3 added SRS
        val geomLayer = shpDataSource.CreateLayer("geom", dstCrs)

        val idField = new org.gdal.ogr.FieldDefn("id", OFTInteger)
        geomLayer.CreateField(idField)
        val featureDefn = geomLayer.GetLayerDefn()
        val feature = new Feature(featureDefn)
        feature.SetGeometry(geom)
        feature.SetField("id", 1)
        geomLayer.CreateFeature(feature)

        flushAndDestroy(shpDataSource) // flush cache

        shapePath
    }

    /**
      * Cleans up the clipper shapefile.
      *
      * @param shapePath
      *   The shapefile to clean up.
      */
    def cleanUpClipper(shapePath: String): Unit = {
        Try(ogr.GetDriverByName("ESRI Shapefile").DeleteDataSource(shapePath))
        Try(gdal.Unlink(shapePath))
    }

}
