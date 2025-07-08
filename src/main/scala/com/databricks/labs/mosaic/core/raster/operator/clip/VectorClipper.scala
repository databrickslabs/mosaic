package com.databricks.labs.mosaic.core.raster.operator.clip

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.utils.PathUtils
import org.gdal.osr.SpatialReference

import java.nio.file.{Files, Paths}

/**
  * VectorClipper is an object that defines the interface for managing a clipper
  * shapefile used for clipping a raster by a vector geometry.
  */
object VectorClipper {

    /**
      * Generates a clipper shapefile that is used to clip a raster. The
      * shapefile is flushed to disk and then the data source is deleted. The
      * shapefile is accessed by gdalwarp by file name.
      * @note
      *   The shapefile is generated in memory.
      *
      * @param geometry
      *   The geometry to clip by.
      * @param geomCRS
      *   The geometry CRS.
      * @param raster
      *   The raster that will be clipped.
      * @param geometryAPI
      *   The geometry API.
      * @return
      *   The shapefile name.
      */
    def generateClipper(geometry: MosaicGeometry, geomCRS: SpatialReference, raster: MosaicRasterGDAL, geometryAPI: GeometryAPI): String = {
        val adjustedGeom = getClipperGeom(geometry, geomCRS, raster, geometryAPI)
        val wkt = adjustedGeom.toWKT
        val tmpFileName = PathUtils.createTmpFilePath("csv")
        val tmpFile = Paths.get(tmpFileName)
        val writer = Files.newBufferedWriter(tmpFile)
        try {
            writer.write(s"""|id,WKT
                             |1,"$wkt"""".stripMargin)
        } finally {
            writer.close()
        }
        tmpFile.toAbsolutePath.toString
    }

    def getClipperGeom(
        geometry: MosaicGeometry,
        geomCRS: SpatialReference,
        raster: MosaicRasterGDAL,
        geometryAPI: GeometryAPI
    ): MosaicGeometry = {
        val rasterCRS = raster.getSpatialReference
        val geomSrcCRS = if (geomCRS == null) rasterCRS else geomCRS
        val projectedGeom = geometry.osrTransformCRS(geomSrcCRS, rasterCRS, geometryAPI)
        val factor = 0.5 * raster.pixelDiagSize
        val pixelArea = Math.abs(raster.pixelXSize * raster.pixelYSize)
        val adjustedGeom = if (projectedGeom.getArea < pixelArea) projectedGeom.buffer(factor) else projectedGeom
        adjustedGeom
    }

    /**
      * Cleans up the clipper file.
      *
      * @param fileName
      *   The file to clean up.
      */
    def cleanUpClipper(fileName: String): Unit = {
        Files.deleteIfExists(Paths.get(fileName))
    }

}
