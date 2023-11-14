package com.databricks.labs.mosaic.utils

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.functions.MosaicContext

import java.nio.file.{Files, Paths}

object PathUtils {

    def getCleanPath(path: String): String = {
        val cleanPath = path.replace("file:/", "/").replace("dbfs:/", "/dbfs/")
        if (cleanPath.endsWith(".zip") || cleanPath.contains(".zip:")) {
            getZipPath(cleanPath)
        } else {
            cleanPath
        }
    }

    def isSubdataset(path: String): Boolean = {
        path.split(":").length == 3
    }

    def getSubdatasetPath(path: String): String = {
        // Subdatasets are paths with a colon in them.
        // We need to check for this condition and handle it.
        // Subdatasets paths are formatted as: "FORMAT:/path/to/file.tif:subdataset"
        val format :: filePath :: subdataset :: Nil = path.split(":").toList
        val isZip = filePath.endsWith(".zip")
        val vsiPrefix = if (isZip) "/vsizip/" else ""
        s"$format:$vsiPrefix$filePath:$subdataset"
    }

    def getZipPath(path: String): String = {
        // It is really important that the resulting path is /vsizip// and not /vsizip/
        // /vsizip// is for absolute paths /viszip/ is relative to the current working directory
        // /vsizip/ wont work on a cluster
        // see: https://gdal.org/user/virtual_file_systems.html#vsizip-zip-archives
        val isZip = path.endsWith(".zip")
        val readPath = if (path.startsWith("/vsizip/")) path else if (isZip) s"/vsizip/$path" else path
        readPath
    }

    def createTmpFilePath(extension: String): String = {
        val tmpDir = MosaicContext.tmpDir
        val uuid = java.util.UUID.randomUUID.toString
        val outPath = s"$tmpDir/raster_${uuid.replace("-", "_")}.$extension"
        Files.createDirectories(Paths.get(outPath).getParent)
        outPath
    }

    def fromSubdatasetPath(path: String): String = {
        val _ :: filePath :: _ :: Nil = path.split(":").toList
        var result = filePath
        if (filePath.startsWith("\"")) result = result.drop(1)
        if (filePath.endsWith("\"")) result = result.dropRight(1)
        result
    }

    def copyToTmp(inPath: String): String = {
        val cleanPath = getCleanPath(inPath)
        val copyFromPath = inPath.replace("file:/", "/").replace("dbfs:/", "/dbfs/")
        val driver = MosaicRasterGDAL.identifyDriver(cleanPath)
        val extension = if (inPath.endsWith(".zip")) "zip" else GDAL.getExtension(driver)
        val tmpPath = createTmpFilePath(extension)
        Files.copy(Paths.get(copyFromPath), Paths.get(tmpPath))
        tmpPath
    }

}
