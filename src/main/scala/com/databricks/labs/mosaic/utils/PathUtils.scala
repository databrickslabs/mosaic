package com.databricks.labs.mosaic.utils

import com.databricks.labs.mosaic.functions.MosaicContext

import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters._

object PathUtils {

    val NO_PATH_STRING = "no_path"

    def replaceDBFSTokens(path: String): String = {
        path
            .replace("file:/", "/")
            .replace("dbfs:/Volumes", "/Volumes")
            .replace("dbfs:/", "/dbfs/")
    }

    def getCleanPath(path: String): String = {
        val cleanPath = replaceDBFSTokens(path)
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
        val tmpDir = MosaicContext.tmpDir(null)
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

    def getStemRegex(path: String): String = {
        val cleanPath = replaceDBFSTokens(path)
        val fileName = Paths.get(cleanPath).getFileName.toString
        val stemName = fileName.substring(0, fileName.lastIndexOf("."))
        val stemEscaped = stemName.replace(".", "\\.")
        val stemRegex = s"$stemEscaped\\..*".r
        stemRegex.toString
    }
    
    def copyToTmpWithRetry(inPath: String, retries: Int = 3): String = {
        var tmpPath = copyToTmp(inPath)
        var i = 0
        while (Files.notExists(Paths.get(tmpPath)) && i < retries) {
            tmpPath = copyToTmp(inPath)
            i += 1
        }
        tmpPath
    }

    def copyToTmp(inPath: String): String = {
        val copyFromPath = replaceDBFSTokens(inPath)
        val inPathDir = Paths.get(copyFromPath).getParent.toString

        val fullFileName = copyFromPath.split("/").last
        val stemRegex = getStemRegex(inPath)

        wildcardCopy(inPathDir, MosaicContext.tmpDir(null), stemRegex)

        s"${MosaicContext.tmpDir(null)}/$fullFileName"
    }

    def wildcardCopy(inDirPath: String, outDirPath: String, pattern: String): Unit = {
        import org.apache.commons.io.FileUtils
        val copyFromPath = replaceDBFSTokens(inDirPath)
        val copyToPath = replaceDBFSTokens(outDirPath)

        val toCopy = Files
            .list(Paths.get(copyFromPath))
            .filter(_.getFileName.toString.matches(pattern))
            .collect(java.util.stream.Collectors.toList[Path])
            .asScala
        
        for (path <- toCopy) {
            val destination = Paths.get(copyToPath, path.getFileName.toString)
            // noinspection SimplifyBooleanMatch
            if (Files.isDirectory(path)) FileUtils.copyDirectory(path.toFile, destination.toFile)
            else FileUtils.copyFile(path.toFile, destination.toFile)
        }
    }

    def parseUnzippedPathFromExtracted(lastExtracted: String, extension: String): String = {
        val trimmed = lastExtracted.replace("extracting: ", "").replace(" ", "")
        val indexOfFormat = trimmed.indexOf(s".$extension/")
        trimmed.substring(0, indexOfFormat + extension.length + 1)
    }

}
