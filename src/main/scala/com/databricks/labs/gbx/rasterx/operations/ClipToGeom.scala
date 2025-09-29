package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.GDAL
import com.databricks.labs.gbx.rasterx.operator.GDALWarp
import com.databricks.labs.gbx.util.NodeFilePathUtil
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.gdal.gdal.Dataset
import org.gdal.osr.SpatialReference
import org.locationtech.jts.geom.Geometry

import java.nio.file.{Files, Paths}
import java.util.Locale

object ClipToGeom {

    def clip(
        ds: Dataset,
        options: Map[String, String],
        geometry: Geometry,
        geomSR: SpatialReference,
        cutlineAllTouched: Boolean = true
    ): (Dataset, Map[String, String]) = {
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
        val outShortName = ds.GetDriver().getShortName
        val extension = GDAL.getExtension(outShortName)
        val resultFileName = s"/vsimem/clip_to_geom_$uuid.$extension"

        val cutlineToken = cutlineAllTouched.toString.toUpperCase(Locale.ROOT)
        val clipperFile = generateClipFile(geometry, geomSR, ds)

        // For -wo consult https://gdal.org/doxygen/structGDALWarpOptions.html
        val result = GDALWarp.executeWarp(
          resultFileName,
          Array(ds),
          options,
          // -q flag for quiet, as there is CRS warning spam
          // we already ensured geoms are in Raster CRS
          command = s"gdalwarp -q -wo CUTLINE_ALL_TOUCHED=$cutlineToken -cutline $clipperFile -crop_to_cutline -oo GEOM_POSSIBLE_NAMES=WKT"
        )
        cleanUpClipper(clipperFile)
        result
    }

    private def generateClipFile(
        geometry: Geometry,
        geomCRS: SpatialReference,
        ds: Dataset
    ): String = {
        val adjustedGeom = getClipGeom(geometry, geomCRS, ds)
        val wkt = JTS.toWKT(adjustedGeom)
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
        val tmpFileName = s"${NodeFilePathUtil.rootPath}/$uuid/clip_$uuid.csv"
        val tmpPrjName = s"${NodeFilePathUtil.rootPath}/$uuid/clip_$uuid.prj"
        val tmpFile = Paths.get(tmpFileName)
        val tmpPrj = Paths.get(tmpPrjName)
        Files.createDirectories(tmpFile.getParent)
        val writer = Files.newBufferedWriter(tmpFile)
        try {
            writer.write(s"""|id,WKT
                             |1,"$wkt"""".stripMargin)
        } finally {
            writer.close()
        }
        Files.writeString(tmpPrj, ds.GetSpatialRef.ExportToWkt)
        tmpFile.toAbsolutePath.toString
    }

    private def getClipGeom(
        geometry: Geometry,
        geomSR: SpatialReference,
        ds: Dataset
    ): Geometry = {
        val dsSR = ds.GetSpatialRef
        val geomSrcSR = if (geomSR == null) dsSR else geomSR
        val projectedGeom = OSRTransformGeometry.transform(geometry, geomSrcSR, dsSR)
        val pxXSize = Math.abs(ds.GetGeoTransform()(1))
        val pxYSize = Math.abs(ds.GetGeoTransform()(5))
        val pxDiagSize = Math.sqrt(pxXSize * pxXSize + pxYSize * pxYSize)
        val factor = 0.5 * pxDiagSize
        val pxArea = Math.abs(pxXSize * pxYSize)
        val adjustedGeom = if (projectedGeom.getArea < pxArea) projectedGeom.buffer(factor) else projectedGeom
        adjustedGeom
    }

    private def cleanUpClipper(fileName: String): Unit = {
        val clipFile = Paths.get(fileName)
        Files.deleteIfExists(clipFile)
        val prjFile = Paths.get(fileName.replace(".csv", ".prj"))
        Files.deleteIfExists(prjFile)
        Files.deleteIfExists(clipFile.getParent)
    }

}
