package com.databricks.labs.mosaic.core.raster.operator.rasterize

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.point.MosaicPoint
import com.databricks.labs.mosaic.core.raster.gdal.{MosaicRasterGDAL, MosaicRasterWriteOptions}
import com.databricks.labs.mosaic.core.raster.operator.gdal.OperatorOptions
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum
import com.databricks.labs.mosaic.utils.PathUtils
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants
import org.gdal.ogr.ogr.{CreateGeometryFromWkb, GetDriverByName}
import org.gdal.ogr.ogrConstants.{OFTReal, wkbPoint, wkbPolygon}
import org.gdal.ogr.{DataSource, Feature, FieldDefn, ogr}

object GDALRasterize {

    private val layerName = "FEATURES"
    private val valueFieldName = "VALUES"

    def executeRasterize(
        geoms: Seq[MosaicGeometry],
        values: Option[Seq[Double]],
        origin: MosaicPoint,
        xWidth: Int,
        yWidth: Int,
        xSize: Double,
        ySize: Double,
        noDataValue: Int = (-99999)
    ): MosaicRasterGDAL = {

        val valuesToBurn = values.getOrElse(geoms.map(_.getAnyPoint.getZ)) // can come back and make this the mean
        val vecDataSource = writeToDataSource(geoms, valuesToBurn, None)

        gdal.AllRegister()
        val writeOptions = MosaicRasterWriteOptions.GTiff
        val outputPath = PathUtils.createTmpFilePath(writeOptions.format)
        val driver = gdal.GetDriverByName(writeOptions.format)
        val newRaster = driver.Create(outputPath, xWidth, yWidth, 1, gdalconstConstants.GDT_Float64)
        newRaster.FlushCache()

        newRaster.SetSpatialRef(vecDataSource.GetLayer(0).GetSpatialRef())
        newRaster.SetGeoTransform(Array(origin.getX, xSize, 0.0, origin.getY, 0.0, ySize))

        val outputBand = newRaster.GetRasterBand(1)
        outputBand.SetNoDataValue(noDataValue)

        val command = s"gdal_rasterize ATTRIBUTE=$valueFieldName"
        val effectiveCommand = OperatorOptions.appendOptions(command, writeOptions)
        val bands = Array(1)
        val burnValues = Array(0.0)
        val rasterizeOptionsVec = OperatorOptions.parseOptions(effectiveCommand)
        gdal.RasterizeLayer(newRaster, bands, vecDataSource.GetLayer(0), burnValues, rasterizeOptionsVec)
        outputBand.FlushCache()

        newRaster.FlushCache()
        newRaster.delete()
        val errorMsg = gdal.GetLastErrorMsg
        val createInfo = Map(
          "path" -> outputPath,
          "parentPath" -> "",
          "driver" -> writeOptions.format,
          "last_command" -> effectiveCommand,
          "last_error" -> errorMsg,
          "all_parents" -> ""
        )
        MosaicRasterGDAL.readRaster(createInfo)
    }

    def writeToDataSource(
        geoms: Seq[MosaicGeometry],
        valuesToBurn: Seq[Double],
        geometryType: Option[GeometryTypeEnum.Value],
        format: String="Memory",
        path: String="mem"
    ): DataSource = {
        ogr.RegisterAll()

        val vecDriver = GetDriverByName(format)
        val vecDataSource = vecDriver.CreateDataSource(path)

        val ogrGeometryType = geometryType.getOrElse(GeometryTypeEnum.fromString(geoms.head.getGeometryType)) match {
            case GeometryTypeEnum.POINT   => wkbPoint
            case GeometryTypeEnum.POLYGON => wkbPolygon
            case _ => throw new UnsupportedOperationException("Only Point and Polygon geometries are supported for rasterization.")
        }

        val layer = vecDataSource.CreateLayer(layerName, geoms.head.getSpatialReferenceOSR, ogrGeometryType)

        val attributeField = new FieldDefn(valueFieldName, OFTReal)
        layer.CreateField(attributeField)

        geoms
            .zip(valuesToBurn)
            .foreach({ case (g: MosaicGeometry, v: Double) =>
                val geom = CreateGeometryFromWkb(g.toWKB)
                val featureDefn = layer.GetLayerDefn()
                val feature = new Feature(featureDefn)
                feature.SetGeometry(geom)
                feature.SetField(valueFieldName, v)
                layer.CreateFeature(feature)
            })

        layer.SyncToDisk()
        layer.delete()
        vecDataSource
    }

}
