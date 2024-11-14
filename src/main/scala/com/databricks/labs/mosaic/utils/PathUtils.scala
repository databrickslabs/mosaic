package com.databricks.labs.mosaic.utils

import com.databricks.labs.mosaic.MOSAIC_RASTER_TMP_PREFIX_DEFAULT
import com.databricks.labs.mosaic.functions.MosaicContext

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{Files, Path, Paths}
import java.time.Clock
import scala.jdk.CollectionConverters._
import scala.util.Try

object PathUtils {

    val NO_PATH_STRING = "no_path"
    val FILE_TOKEN = "file:"
    val VSI_ZIP_TOKEN = "/vsizip/"
    val DBFS_FUSE_TOKEN = "/dbfs"
    val DBFS_TOKEN = "dbfs:"
    val VOLUMES_TOKEN = "/Volumes"
    val WORKSPACE_TOKEN = "/Workspace"

    /**
      * Cleans up variations of path.
      * - handles subdataset path
      * - handles "aux.xml" sidecar file
      * - handles zips, including "/vsizip/"
      * @param path
      */
    def cleanUpPath(path: String): Unit = {
        // 0.4.3 - new function
        val isSD = isSubdataset(path)
        val filePath = if (isSD) fromSubdatasetPath(path) else path
        val pamFilePath = s"$filePath.aux.xml"
        val cleanPath = filePath.replace(VSI_ZIP_TOKEN, "")
        val zipPath = if (cleanPath.endsWith("zip")) cleanPath else s"$cleanPath.zip"

        Try(Files.deleteIfExists(Paths.get(cleanPath)))
        Try(Files.deleteIfExists(Paths.get(path)))
        Try(Files.deleteIfExists(Paths.get(filePath)))
        Try(Files.deleteIfExists(Paths.get(pamFilePath)))
        if (Files.exists(Paths.get(zipPath))) {
            Try(Files.deleteIfExists(Paths.get(zipPath.replace(".zip", ""))))
        }
        Try(Files.deleteIfExists(Paths.get(zipPath)))
        collectEmptyTmpDirs()
    }

    private def collectEmptyTmpDirs(): Unit = this.synchronized {
        // iterate over all the directories in the temp location and delete any that are empty
        // and older than 10 seconds
        // This needs to be thread safe so we don't try and probe a directory
        // that has been deleted in another thread
        val tmpDir = Paths.get(MosaicContext.tmpDir(null)).getParent
        if (Files.exists(tmpDir)) {
            tmpDir.toFile
                .listFiles
                .filter(_.isDirectory)
                .filter({ f =>
                    val attrs = Files.readAttributes(Paths.get(f.getAbsolutePath), classOf[BasicFileAttributes])
                    val lastModifiedTime = attrs.lastModifiedTime().toInstant
                    Clock.systemDefaultZone().instant().minusSeconds(10).isAfter(lastModifiedTime)
                })
                .filter(_.listFiles.isEmpty)
                .foreach({ f => Try(f.delete()) })
        }
    }

    /**
      * Copy provided path to tmp.
     *
     * @param inPath
      *   Path to copy from.
      * @return
      *   The copied path.
      */
    def copyToTmp(inPath: String): String = {
        val copyFromPath = replaceDBFSTokens(inPath)
        val inPathDir = Paths.get(copyFromPath).getParent.toString

        val fullFileName = copyFromPath.split("/").last
        val stemRegex = getStemRegex(inPath)

        wildcardCopy(inPathDir, MosaicContext.tmpDir(null), stemRegex)

        s"${MosaicContext.tmpDir(null)}/$fullFileName"
    }

    /**
      * Copy path to tmp with retries.
      * @param inPath
      *   Path to copy from.
      * @param retries
      *   How many times to retry copy, default = 3.
      * @return
      *   The tmp path.
      */
    def copyToTmpWithRetry(inPath: String, retries: Int = 3): String = {
        var tmpPath = copyToTmp(inPath)
        var i = 0
        while (Files.notExists(Paths.get(tmpPath)) && i < retries) {
            tmpPath = copyToTmp(inPath)
            i += 1
        }
        tmpPath
    }

    /**
      * Create a file under tmp dir.
      * - Directories are created.
      * - File itself is not create.
      * @param extension
      *   The file extension to use.
      * @return
      *   The tmp path.
      */
    def createTmpFilePath(extension: String): String = {
        val tmpDir = MosaicContext.tmpDir(null)
        val uuid = java.util.UUID.randomUUID.toString
        val outPath = s"$tmpDir/raster_${uuid.replace("-", "_")}.$extension"
        Files.createDirectories(Paths.get(outPath).getParent)
        outPath
    }

    /**
      * File path which had a subdataset.
      * - split on ":" and return just the path,
      *   not the subdataset.
      * - remove any quotes at start and end.
      * @param path
      *   Provided path.
      * @return
      *   The path without subdataset.
      */
    def fromSubdatasetPath(path: String): String = {
        val _ :: filePath :: _ :: Nil = path.split(":").toList
        var result = filePath
        if (filePath.startsWith("\"")) result = result.drop(1)
        if (filePath.endsWith("\"")) result = result.dropRight(1)
        result
    }

    /**
      * Generate regex string of path filename.
      * - handles fuse paths.
      * - handles "." in the filename "stem".
      * @param path
      *   Provided path.
      * @return
      *   Regex string.
      */
    def getStemRegex(path: String): String = {
        val cleanPath = replaceDBFSTokens(path)
        val fileName = Paths.get(cleanPath).getFileName.toString
        val stemName = fileName.substring(0, fileName.lastIndexOf("."))
        val stemEscaped = stemName.replace(".", "\\.")
        val stemRegex = s"$stemEscaped\\..*".r
        stemRegex.toString
    }

    /**
      * Get subdataset path.
      * - these paths end with ":subdataset".
      * - adds "/vsizip/" if needed.
      * @param path
      *   Provided path.
      * @return
      *   Standardized path.
      */
    def getSubdatasetPath(path: String): String = {
        // Subdatasets are paths with a colon in them.
        // We need to check for this condition and handle it.
        // Subdatasets paths are formatted as: "FORMAT:/path/to/file.tif:subdataset"
        val format :: filePath :: subdataset :: Nil = path.split(":").toList
        val isZip = filePath.endsWith(".zip")
        val vsiPrefix = if (isZip) VSI_ZIP_TOKEN else ""
        s"$format:$vsiPrefix$filePath:$subdataset"
    }



    /**
      * Clean path.
      * - handles fuse paths.
      * - handles zip paths
      * @param path
      *   Provided path.
      * @return
      *   Standardized string.
      */
    def getCleanPath(path: String): String = {
        val cleanPath = replaceDBFSTokens(path)
        if (cleanPath.endsWith(".zip") || cleanPath.contains(".zip:")) {
            getZipPath(cleanPath)
        } else {
            cleanPath
        }
    }

    /**
      * Standardize zip paths.
      *  - Add "/vsizip/" as needed.
      * @param path
      *   Provided path.
      * @return
      *   Standardized path.
      */
    def getZipPath(path: String): String = {
        // It is really important that the resulting path is /vsizip// and not /vsizip/
        // /vsizip// is for absolute paths /viszip/ is relative to the current working directory
        // /vsizip/ wont work on a cluster
        // see: https://gdal.org/user/virtual_file_systems.html#vsizip-zip-archives
        val isZip = path.endsWith(".zip")
        val readPath = if (path.startsWith(VSI_ZIP_TOKEN)) path else if (isZip) s"$VSI_ZIP_TOKEN$path" else path
        readPath
    }

    /**
      * Test for whether path is in a fuse location,
      * looks ahead somewhat beyond DBFS.
      * - handles DBFS, Volumes, and Workspace paths.
      * @param path
      *   Provided path.
      * @return
      *   True if path is in a fuse location.
      */
    def isFuseLocation(path: String): Boolean = {
        // 0.4.3 - new function
        val p = getCleanPath(path)
        val isFuse = p match {
            case _ if (
                p.startsWith(s"$DBFS_FUSE_TOKEN/") ||
                    p.startsWith(s"$VOLUMES_TOKEN/") ||
                    p.startsWith(s"$WORKSPACE_TOKEN/")) => true
            case _ => false
        }
        isFuse
    }

    /**
      * Test for whether path is in the temp location.
      * @param path
      *   Provided path.
      * @return
      *   True if path is in a temp location.
      */
    def isTmpLocation(path: String): Boolean = {
        val p = getCleanPath(path)
        p.startsWith(MOSAIC_RASTER_TMP_PREFIX_DEFAULT)
    }

    /**
      * Is the path a subdataset?
      * - Known by ":" after the filename.
      * @param path
      *   Provided path.
      * @return
      *   True if is a subdataset.
      */
    def isSubdataset(path: String): Boolean = {
        path.split(":").length == 3
    }

    /**
      * Parse the unzipped path from standard out.
      *  - Called after a prompt is executed to unzip.
      * @param lastExtracted
      *   Standard out line to parse.
      * @param extension
      *   Extension of file, e.g. "shp".
      * @return
      *   The parsed path.
      */
    def parseUnzippedPathFromExtracted(lastExtracted: String, extension: String): String = {
        val trimmed = lastExtracted.replace("extracting: ", "").replace(" ", "")
        val indexOfFormat = trimmed.indexOf(s".$extension/")
        trimmed.substring(0, indexOfFormat + extension.length + 1)
    }

    /**
      * Replace various file path schemas that are not needed
      * for internal / local handling.
      * - handles "file:". "dbfs:"
      * - appropriate for "/dbfs/", "/Volumes/", and "/Workspace/"
      *   paths, which can be read locally.
      * @param path
      *   Provided path.
      * @return
      *   Replaced string.
      */
    def replaceDBFSTokens(path: String): String = {
        path
            .replace(s"$FILE_TOKEN/", "/")
            .replace(s"$DBFS_TOKEN$VOLUMES_TOKEN/", s"$VOLUMES_TOKEN/")
            .replace(s"$DBFS_TOKEN/", s"$DBFS_FUSE_TOKEN/")
    }

    /**
      * Perform a wildcard copy.
      * @param inDirPath
      *   Provided in dir.
      * @param outDirPath
      *   Provided out dir.
      * @param pattern
      *   Regex pattern to match.
      */
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
            if (path != destination) {
                if (Files.isDirectory(path)) {
                    FileUtils.copyDirectory(path.toFile, destination.toFile)
                }
                else {
                    FileUtils.copyFile(path.toFile, destination.toFile)
                }
            }
        }
    }

}
