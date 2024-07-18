package com.databricks.labs.mosaic.core.raster.operator.rasterize

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.point.MosaicPoint
import com.databricks.labs.mosaic.core.raster.gdal.{MosaicRasterGDAL, MosaicRasterWriteOptions}
import com.databricks.labs.mosaic.core.raster.operator.gdal.OperatorOptions
import com.databricks.labs.mosaic.utils.PathUtils
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants
import org.gdal.ogr.ogr.{CreateGeometryFromWkb, GetDriverByName}
import org.gdal.ogr.{DataSource, Feature, FieldDefn, ogr}
import org.gdal.ogr.ogrConstants.{wkbPoint, OFTReal}

import java.nio.file.{Files, Paths}

object GDALRasterize {

    def executeRasterize(
        geoms: Seq[MosaicGeometry],
        values: Option[Seq[Double]],
        origin: MosaicPoint,
        xWidth: Int,
        yWidth: Int,
        xSize: Double,
        ySize: Double,
        noDataValue: Int = (-9999)
    ): MosaicRasterGDAL = {

        ogr.RegisterAll()
        val vecDriver = GetDriverByName("Memory")
        val vecDataSource = vecDriver.CreateDataSource("mem")

        val layerName = "FEATURES"
        val valueFieldName = "VALUES"
        val layer = vecDataSource.CreateLayer(layerName, geoms.head.getSpatialReferenceOSR, wkbPoint)

        val attributeField = new FieldDefn(valueFieldName, OFTReal)
        layer.CreateField(attributeField)

        val valuesToBurn = values.getOrElse(geoms.map(_.getAnyPoint.getZ)) // can come back and make this the mean

        geoms.zip(valuesToBurn)
            .foreach({
                case (g: MosaicGeometry, v: Double) =>
                val geom = CreateGeometryFromWkb(g.toWKB)
                val featureDefn = layer.GetLayerDefn()
                val feature = new Feature(featureDefn)
                feature.SetGeometry(geom)
                feature.SetField(valueFieldName, v)
                layer.CreateFeature(feature)
            })

        gdal.AllRegister()
        val writeOptions = MosaicRasterWriteOptions()
        val outputPath = PathUtils.createTmpFilePath(writeOptions.format)
        val driver = gdal.GetDriverByName("GTiff")
        val newRaster = driver.Create(outputPath, xWidth, yWidth, 1, gdalconstConstants.GDT_Float32)

        newRaster.SetSpatialRef(vecDataSource.GetLayer(0).GetSpatialRef())
        newRaster.SetGeoTransform(Array(origin.getX, xSize, 0.0, origin.getY, 0.0, ySize))
        newRaster.GetRasterBand(1).SetNoDataValue(noDataValue)

        val command = s"gdal_rasterize ATTRIBUTE=$valueFieldName"
        val effectiveCommand = OperatorOptions.appendOptions(command, writeOptions)
        val bands = Array(1)
        val burnValues = Array(0.0)
        val rasterizeOptionsVec = OperatorOptions.parseOptions(effectiveCommand)
        gdal.RasterizeLayer(newRaster, bands, vecDataSource.GetLayer(0), burnValues, rasterizeOptionsVec)
        newRaster.FlushCache()

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

}
