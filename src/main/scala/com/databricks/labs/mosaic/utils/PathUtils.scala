package com.databricks.labs.mosaic.utils

import java.nio.file.{Files, Paths}
import java.util.UUID

object PathUtils {

    def getFormatExtension(rawPath: String): String = {
        val path: String = resolvePath(rawPath)
        val fileName = path.split("/").last
        val extension = fileName.split("\\.").last
        extension
    }

    private def resolvePath(rawPath: String): String = {
        val path =
            if (isSubdataset(rawPath)) {
                val _ :: filePath :: _ :: Nil = rawPath.split(":").toList
                filePath
            } else {
                rawPath
            }
        path
    }

    def getCleanPath(path: String, useZipPath: Boolean): String = {
        val cleanPath = path.replace("file:/", "/").replace("dbfs:/", "/dbfs/")
        if (useZipPath && cleanPath.endsWith(".zip")) {
            getZipPath(cleanPath)
        } else {
            cleanPath
        }
    }

    def isSubdataset(path: String): Boolean = {
        path.split(":").length == 3
    }

    def isInMemory(path: String): Boolean = {
        path.startsWith("/vsimem/") || path.contains("/vsimem/")
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

    def copyToTmp(rawPath: String): String = {
        try {
            val path: String = resolvePath(rawPath)

            val fileName = path.split("/").last
            val extension = getFormatExtension(path)

            val inPath = getCleanPath(path, useZipPath = extension == "zip")

            val randomID = UUID.randomUUID().toString
            val tmpDir = Files.createTempDirectory(s"mosaic_local_$randomID").toFile.getAbsolutePath

            val outPath = s"$tmpDir/$fileName"

            Files.createDirectories(Paths.get(tmpDir))
            Files.copy(Paths.get(inPath), Paths.get(outPath))

            if (isSubdataset(rawPath)) {
                val format :: _ :: subdataset :: Nil = rawPath.split(":").toList
                getSubdatasetPath(s"$format:$outPath:$subdataset")
            } else {
                outPath
            }
        } catch {
            case _: Throwable => rawPath
        }
    }

    def createTmpFilePath(uuid: String, extension: String): String = {
        val randomID = UUID.randomUUID()
        val tmpDir = Files.createTempDirectory(s"mosaic_tmp_$randomID").toFile.getAbsolutePath
        val outPath = s"$tmpDir/raster_${uuid.replace("-", "_")}.$extension"
        Files.createDirectories(Paths.get(outPath).getParent)
        outPath
    }

    def fromSubdatasetPath(path: String): String = {
        val _ :: filePath :: _ :: Nil = path.split(":").toList
        filePath
    }

}
