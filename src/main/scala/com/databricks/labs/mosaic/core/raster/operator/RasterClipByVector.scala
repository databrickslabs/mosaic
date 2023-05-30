package com.databricks.labs.mosaic.core.raster.operator

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.raster.{MosaicRaster, MosaicRasterGDAL}
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.POLYGON
import org.gdal.gdal.{WarpOptions, gdal}
import org.gdal.ogr.{Feature, ogr}
import org.gdal.ogr.ogrConstants._
import org.gdal.osr.SpatialReference

object RasterClipByVector {

    def clip(raster: MosaicRaster, geometry: MosaicGeometry, geometryAPI: GeometryAPI): MosaicRaster = {
        val rasterCRS = raster.getRaster.GetSpatialRef()
        val geomCRS = new SpatialReference()
        geomCRS.ImportFromEPSG(geometry.getSpatialReference)

        val uuid = java.util.UUID.fromString(geometry.toWKT)
        val shapeFileName = s"/tmp/${uuid.toString}.shp"
        val tifFileName = s"/tmp/${uuid.toString}.tif"

        val shpDriver = ogr.GetDriverByName("ESRI Shapefile")
        val shpDataSource = shpDriver.CreateDataSource(shapeFileName)

        val geomLayer = shpDataSource.CreateLayer("geom", geomCRS)

        val osrGeom = geometryAPI
            .geometry(geometry.getShellPoints.head.map(p => geometryAPI.fromCoords(p.asSeq.reverse)), POLYGON)
            .osrTransformCRS(geomCRS, rasterCRS, geometryAPI)
        val geom = ogr.CreateGeometryFromWkb(osrGeom.toWKB)

        val idField = new org.gdal.ogr.FieldDefn("id", OFTInteger)
        geomLayer.CreateField(idField)
        val featureDefn = geomLayer.GetLayerDefn()
        val feature = new Feature(featureDefn)
        feature.SetGeometry(geom)
        feature.SetField("id", uuid.toString)
        geomLayer.CreateFeature(feature)
        shpDataSource.FlushCache()

        val warpOptionsVec = new java.util.Vector[String]()
        warpOptionsVec.add("-cutline")
        warpOptionsVec.add(shapeFileName)
        warpOptionsVec.add("-cwhere")
        warpOptionsVec.add(s"id='$uuid'")
        warpOptionsVec.add("-crop_to_cutline")
        val warpOptions = new WarpOptions(warpOptionsVec)

        val result = gdal.Warp(tifFileName, Array(raster.getRaster), warpOptions)
        result.FlushCache()
        val xSizeRes = result.GetRasterXSize()
        val ySizeRes = result.GetRasterYSize()

        val outputDs = gdal.Open(tifFileName)
        outputDs.SetGeoTransform(raster.getGeoTransform)
        MosaicRasterGDAL(outputDs, tifFileName, 2 * xSizeRes * ySizeRes * raster.numBands)

    }

}
