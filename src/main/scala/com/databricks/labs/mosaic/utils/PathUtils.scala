package com.databricks.labs.mosaic.utils

import com.databricks.labs.mosaic.functions.{MosaicContext, ExprConfig}
import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters._
import scala.util.Try

object PathUtils {

    val FILE_TOKEN = "file:"
    val VSI_ZIP_TOKEN = "/vsizip/"
    val DBFS_FUSE_TOKEN = "/dbfs"
    val DBFS_TOKEN = "dbfs:"
    val VOLUMES_TOKEN = "/Volumes"
    val WORKSPACE_TOKEN = "/Workspace"

    val URI_TOKENS = Seq(FILE_TOKEN, DBFS_TOKEN)

    /**
     * For clarity, this is the function to call when you want a path that could actually be found on the file system.
     * - simply calls `getCleanPath` with 'addVsiZipToken' set to false.
     * - non guarantees the path actually exists.
     *
     * @param rawPath
     *   Path to clean for file system.
     *
     * @return
     *   Cleaned path.
     */
    def asFileSystemPath(rawPath: String): String = getCleanPath(rawPath, addVsiZipToken = false)

    /**
     * Get subdataset GDAL path.
     * - these paths end with ":subdataset".
     * - adds "/vsizip/" if needed.
     * @param rawPath
     *   Provided path.
     * @param uriFuseReady
     *   drop URISchema part and call [[makeURIFuseReady]]
     * @return
     *   Standardized path.
     */
    def asSubdatasetGDALPathOpt(rawPath: String, uriFuseReady: Boolean): Option[String] =
        Try {
            // Subdatasets are paths with a colon in them.
            // We need to check for this condition and handle it.
            // Subdatasets paths are formatted as: "FORMAT:/path/to/file.tif:subdataset"
            if (!isSubdataset(rawPath)) {
                null
            } else {
                val subTokens = getSubdatasetTokenList(rawPath)
                if (startsWithURI(rawPath)) {
                    val uriSchema :: filePath :: subdataset :: Nil = subTokens
                    val isZip = filePath.endsWith(".zip")
                    val vsiPrefix = if (isZip) VSI_ZIP_TOKEN else ""
                    val subPath = s"$uriSchema:$vsiPrefix$filePath:$subdataset"
                    if (uriFuseReady) {
                        // handle uri schema wrt fuse
                        this.makeURIFuseReady(subPath, keepVsiZipToken = true)
                    } else {
                        subPath
                    }
                } else {
                    val filePath :: subdataset :: Nil = subTokens
                    val isZip = filePath.endsWith(".zip")
                    val vsiPrefix = if (isZip) VSI_ZIP_TOKEN else ""
                    // cannot make fuse ready without [[URI_TOKENS]]
                    s"$vsiPrefix$filePath:$subdataset"
                }
            }
        }.toOption

    /**
      * Cleans up variations of path.
      * - 0.4.3 recommend to let CleanUpManager handle local files based on
      *   a session configured file / dir modification age-off policy.
      * - handles subdataset path
      * - handles "aux.xml" sidecar file
      * - handles zips, including "/vsizip/"
      * @param rawPath
      */
    @deprecated("0.4.3 recommend to let CleanUpManager handle")
    def cleanUpPath(rawPath: String): Unit = {
        val cleanPath = getCleanPath(rawPath, addVsiZipToken = false)
        val pamFilePath = s"$cleanPath.aux.xml"

        Try(Files.deleteIfExists(Paths.get(cleanPath)))
        Try(Files.deleteIfExists(Paths.get(rawPath)))
        Try(Files.deleteIfExists(Paths.get(pamFilePath)))
    }

    // scalastyle:off println
    /**
     * Explicit deletion of PAM (aux.xml) files, if found.
     * - Can pass a directory or a file path
     * - Subdataset file paths as well.
     * @param rawPathOrDir
     *   will list directories recursively, will get a subdataset path or a clean path otherwise.
     */
    def cleanUpPAMFiles(rawPathOrDir: String): Unit = {
        if (isSubdataset(rawPathOrDir)) {
            // println(s"... subdataset path detected '$path'")
            Try(Files.deleteIfExists(
                Paths.get(s"${getWithoutSubdatasetName(rawPathOrDir, addVsiZipToken = false)}.aux.xml"))
            )
        } else {
            val cleanPathObj = Paths.get(getCleanPath(rawPathOrDir, addVsiZipToken = false))
            if (Files.isDirectory(cleanPathObj)) {
                // println(s"... directory path detected '$cleanPathObj'")
                cleanPathObj.toFile.listFiles()
                    .filter(f => f.isDirectory || f.toString.endsWith(".aux.xml"))
                    .foreach(f => cleanUpPAMFiles(f.toString))
            } else {
                // println(s"... path detected '$cleanPathObj'")
                if (cleanPathObj.toString.endsWith(".aux.xml")) {
                    Try(Files.deleteIfExists(cleanPathObj))
                } else {
                    Try(Files.deleteIfExists(Paths.get(s"${cleanPathObj.toString}.aux.xml")))
                }
            }
        }
    }
    // scalastyle:on println

    /**
      * Copy provided path to tmp.
      *
      * @param inRawPath
      *   Path to copy from.
      * @param exprConfigOpt
      *   Option [[ExprConfig]].
      * @return
      *   The copied path.
      */
    def copyToTmp(inRawPath: String, exprConfigOpt: Option[ExprConfig]): String = {
        val copyFromPath = makeURIFuseReady(inRawPath, keepVsiZipToken = false)
        val inPathDir = Paths.get(copyFromPath).getParent.toString

        val fullFileName = copyFromPath.split("/").last
        val stemRegex = getStemRegex(inRawPath)

        wildcardCopy(inPathDir, MosaicContext.tmpDir(exprConfigOpt), stemRegex)

        s"${MosaicContext.tmpDir(exprConfigOpt)}/$fullFileName"
    }

    /**
      * Copy path to tmp with retries.
 *
      * @param inCleanPath
      *   Path to copy from.
      * @param retries
      *   How many times to retry copy, default = 3.
      * @param exprConfigOpt
      *   Option [[ExprConfig]].
      * @return
      *   The tmp path.
      */
    def copyCleanPathToTmpWithRetry(inCleanPath: String, exprConfigOpt: Option[ExprConfig], retries: Int = 3): String = {
        var tmpPath = copyToTmp(inCleanPath, exprConfigOpt)
        var i = 0
        while (Files.notExists(Paths.get(tmpPath)) && i < retries) {
            tmpPath = copyToTmp(inCleanPath, exprConfigOpt)
            i += 1
        }
        tmpPath
    }

    /**
      * Create a file under tmp dir.
      * - Directories are created.
      * - File itself is not create.
 *
      * @param extension
      * The file extension to use.
      * @param exprConfigOpt
      *   Option [[ExprConfig]]
      * @return
      *   The tmp path.
      */
    def createTmpFilePath(extension: String, exprConfigOpt: Option[ExprConfig]): String = {
        val tmpDir = MosaicContext.tmpDir(exprConfigOpt)
        val uuid = java.util.UUID.randomUUID.toString
        val outPath = s"$tmpDir/raster_${uuid.replace("-", "_")}.$extension"
        Files.createDirectories(Paths.get(outPath).getParent)
        outPath
    }

    /** @return Returns file extension as option (path converted to clean path). */
    def getExtOptFromPath(path: String): Option[String] =
        Try {
            Paths.get(asFileSystemPath(path)).getFileName.toString.split("\\.").last
        }.toOption

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
        val cleanPath = makeURIFuseReady(path, keepVsiZipToken = false)
        val fileName = Paths.get(cleanPath).getFileName.toString
        val stemName = fileName.substring(0, fileName.lastIndexOf("."))
        val stemEscaped = stemName.replace(".", "\\.")
        val stemRegex = s"$stemEscaped\\..*".r
        stemRegex.toString
    }

    /**
     * Get subdataset name as an option.
     * - subdataset paths end with ":subdataset".
     *
     * @param rawPath
     *   Provided path.
     * @return
     *   Option subdatasetName
     */
    def getSubdatasetNameOpt(rawPath: String): Option[String] =
        Try {
            // Subdatasets are paths with a colon in them.
            // We need to check for this condition and handle it.
            // Subdatasets paths are formatted as: "FORMAT:/path/to/file.tif:subdataset"
            val subTokens = getSubdatasetTokenList(rawPath)
            val result = {
                if (startsWithURI(rawPath)) {
                    val _ :: _ :: subdataset :: Nil = subTokens
                    subdataset
                } else {
                    val _ :: subdataset :: Nil = subTokens
                    subdataset
                }
            }
            result
        }.toOption

    /**
     * Is a path a URI path, i.e. 'file:' or 'dbfs:' for our interests.
     *
     * @param rawPath
     *   To check.
     * @return
     *   Whether the path starts with any [[URI_TOKENS]].
     */
    def startsWithURI(rawPath: String): Boolean = Try {
        URI_TOKENS.exists(rawPath.startsWith)  // <- one element found?
    }.getOrElse(false)

    /**
     * Get Subdataset Tokens
     * - This is to enforce convention.
     *
     * @param rawPath
     *   To split into tokens (based on ':').
     * @return
     *   [[List]] of string tokens from the path.
     */
    def getSubdatasetTokenList(rawPath: String): List[String] =
        Try {
            rawPath.split(":").toList
        }.getOrElse(List.empty[String])

    /**
     * Get path without the subdataset name, if present.
     * - these paths end with ":subdataset".
     * - split on ":" and return just the path,
     *   not the subdataset.
     * - remove any quotes at start and end.
     *
     * @param rawPath
     *   Provided path.
     * @param addVsiZipToken
     *   Whether to include the [[VSI_ZIP_TOKEN]] (true means add it to zips).
     * @return
     *   Standardized path (no [[URI_TOKENS]] or ":subbdataset"
     */
    def getWithoutSubdatasetName(rawPath: String, addVsiZipToken: Boolean): String = {
        // Subdatasets are paths with a colon in them.
        // We need to check for this condition and handle it.
        // Subdatasets paths are formatted as: "FORMAT:/path/to/file.tif:subdataset"
        // Additionally if the path is a zip, the format looks like "FORMAT:/vsizip//path/to/file.zip:subdataset"
        val tokens = getSubdatasetTokenList(rawPath)
        val filePath = {
            if (startsWithURI(rawPath)) {
                // first and second token returned (not subdataset name)
                val uriSchema :: filePath :: _ :: Nil = tokens
                s"$uriSchema:$filePath"
            } else if (tokens.length > 1) {
                // first token returned (not subdataset name)
                val filePath :: _ :: Nil = tokens
                filePath
            } else {
                // single token (no uri or subdataset)
                val filePath :: Nil = tokens
                filePath
            }
        }

        var result = filePath
        // strip quotes
        if (filePath.startsWith("\"")) result = result.drop(1)
        if (filePath.endsWith("\"")) result = result.dropRight(1)

        //handle vsizip
        val isZip = result.endsWith(".zip")
        if (
            addVsiZipToken && isZip && !result.startsWith(VSI_ZIP_TOKEN)
        ){
            result = s"$VSI_ZIP_TOKEN$result"
        } else if (!addVsiZipToken) {
            result = this.replaceVsiZipToken(result)
        }

        result
    }

    /**
      * Clean file path:
      * (1) subdatasets (may be zips)
      * (2) "normal" zips
      * (3) [[URI_TOKENS]] for fuse readiness.
      *
      * @param rawPath
      *   Provided path.
      * @param addVsiZipToken
      *   Specify whether the result should include [[VSI_ZIP_TOKEN]].
      * @return
      *   Standardized file path string.
      */
    def getCleanPath(rawPath: String, addVsiZipToken: Boolean): String = {
        val filePath = {
            if (isSubdataset(rawPath)) getWithoutSubdatasetName(rawPath, addVsiZipToken)    // <- (1) subs (may have URI)
            else if (rawPath.endsWith(".zip")) getCleanZipPath(rawPath, addVsiZipToken)     // <- (2) normal zip
            else rawPath
        }
        // (3) handle [[URI_TOKENS]]
        // - one final assurance of conformity to the expected behavior
        // - mostly catching rawpath and subdataset (as zip path already handled)
        val result =  makeURIFuseReady(filePath, keepVsiZipToken = addVsiZipToken)
        result
    }

    /**
      * Standardize zip paths.
      *  - Add "/vsizip/" as directed.
      *  - Called from `cleanPath`
      *  - Don't call from `path` (if a subdataset)
      *
      * @param path
      *   Provided path.
      * @param addVsiZipToken
      *   Specify whether the result should include [[VSI_ZIP_TOKEN]].
      * @return
      *   Standardized path.
      */
    def getCleanZipPath(path: String, addVsiZipToken: Boolean): String = {

        // (1) handle subdataset path (start by dropping the subdataset name)
        var result = {
            if (isSubdataset(path)) getWithoutSubdatasetName(path, addVsiZipToken = false)
            else path  // <- vsizip handled later (may have a "normal" zip here)
        }
        // (2) handle [[URI_TOKENS]] for FUSE (works with/without [[VIS_ZIP_TOKEN]])
        // - there are no [[URI_TOKENS]] after this.
        result = this.makeURIFuseReady(result, keepVsiZipToken = addVsiZipToken)

        // (3) strip quotes
        if (result.startsWith("\"")) result = result.drop(1)
        if (result.endsWith("\"")) result = result.dropRight(1)

        // (4) if 'addVsiZipToken' true, add [[VSI_ZIP_TOKEN]] to zips; conversely, remove if false
        // - It is really important that the resulting path is /vsizip// and not /vsizip/
        // /vsizip// is for absolute paths /viszip/ is relative to the current working directory
        // /vsizip/ wont work on a cluster.
        // - See: https://gdal.org/user/virtual_file_systems.html#vsizip-zip-archives
        // - There are no [[URI_TOKENS]] now so can just prepend [[VSI_ZIP_TOKEN]].

        if (addVsiZipToken && result.endsWith(".zip") && !this.hasVisZipToken(result)) {
            // final condition where "normal" zip still hasn't had the [[VSI_ZIP_TOKEN]] added
            result = s"$VSI_ZIP_TOKEN$result"
        } else if (!addVsiZipToken) {
            // final condition to strip [[VSI_ZIP_TOKEN]]
            result = this.replaceVsiZipToken(result)
        }

        result
    }

    /** @return whether path contains [[VSI_ZIP_TOKEN]]. */
    def hasVisZipToken(path: String): Boolean = {
        path.contains(VSI_ZIP_TOKEN)
    }

    /**
      * Test for whether path is in a fuse location:
      * - handles DBFS, Volumes, and Workspace paths.
      * - this will clean the path to remove [[URI_TOKENS]]
      * - Should work with Dir as well
      *
      * @param path
      *   Provided path.
      * @return
      *   True if path is in a fuse location.
      */
    def isFusePathOrDir(path: String): Boolean = {
        // clean path strips out "file:" and "dbfs:".
        // also, strips out [[VSI_ZIP_TOKEN]].
        // then can test for start of the actual file path,
        // startswith [[DBFS_FUSE_TOKEN]], [[VOLUMES_TOKEN]], or [[WORKSPACE_TOKEN]].
        // 0.4.3 - new function
        getCleanPath(path, addVsiZipToken = false) match {
            case p if
                p.startsWith(s"$DBFS_FUSE_TOKEN/") ||
                    p.startsWith(s"$VOLUMES_TOKEN/") ||
                    p.startsWith(s"$WORKSPACE_TOKEN/") => true
            case _ => false
        }
    }

    /**
      * Is the path a subdataset?
      * - Known by ":" after the filename.
      * - 0.4.3+  `isURIPath` to know if expecting 1 or 2 ":" in path.
      *
      * @param rawPath
      *   Provided path.
      * @return
      *   True if is a subdataset.
      */
    def isSubdataset(rawPath: String): Boolean = {
        if (startsWithURI(rawPath)) getSubdatasetTokenList(rawPath).length == 3 // <- uri token
        else getSubdatasetTokenList(rawPath).length == 2 // <- no uri token
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
      * Replace various path URI schemas for local FUSE handling.
      * - DON'T PRE-STRIP THE URI SCHEMAS.
      * - strips "file:". "dbfs:" URI Schemas
      *- "dbfs:/..." when not a Volume becomes "/dbfs/".
      * - VALID FUSE PATHS START WITH with "/dbfs/", "/Volumes/", and "/Workspace/"
      *
      * @param rawPath
      *   Provided path.
      * @param keepVsiZipToken
      *   Whether to preserve [[VSI_ZIP_TOKEN]] if present.
      * @return
      *   Replaced string.
      */
    def makeURIFuseReady(rawPath: String, keepVsiZipToken: Boolean): String = {
        // (1) does the path have [[VSI_ZIP_TOKEN]]?
        val hasVsi = this.hasVisZipToken(rawPath)
        // (2) remove [[VSI_ZIP_TOKEN]] and handle fuse tokens
        var result = replaceVsiZipToken(rawPath)
            .replace(s"$FILE_TOKEN/", "/")
            .replace(s"$DBFS_TOKEN$VOLUMES_TOKEN/", s"$VOLUMES_TOKEN/")
            .replace(s"$DBFS_TOKEN/", s"$DBFS_FUSE_TOKEN/")
        // (3) if conditions met, prepend [[VSI_ZIP_TOKEN]]
        if (hasVsi && keepVsiZipToken) {
            result = s"$VSI_ZIP_TOKEN$result"
        }

        result
    }

    /**
     * When properly configured for GDAL, zip paths (including subdatasets) will have [[VSI_ZIP_TOKEN]] added.
     * - this removes that from any provided path.
     *
     * @param path
     *   To replace on.
     * @return
     *   The path without [[VSI_ZIP_TOKEN]].
     */
    def replaceVsiZipToken(path: String): String = {
        path.replace(VSI_ZIP_TOKEN, "")
    }

    /**
      * Perform a wildcard copy.
      * - This is pure file system based operation,
      *   with some regex around the 'pattern'.
      *
      * @param inDirPath
      *   Provided in dir.
      * @param outDirPath
      *   Provided out dir.
      * @param pattern
      *   Regex pattern to match.
      */
    def wildcardCopy(inDirPath: String, outDirPath: String, pattern: String): Unit = {
        import org.apache.commons.io.FileUtils
        val copyFromPath = makeURIFuseReady(inDirPath, keepVsiZipToken = false)
        val copyToPath = makeURIFuseReady(outDirPath, keepVsiZipToken = false)

        val toCopy = Files
            .list(Paths.get(copyFromPath))
            .filter(_.getFileName.toString.matches(pattern))
            .collect(java.util.stream.Collectors.toList[Path])
            .asScala

        for (path <- toCopy) {
            val destination = Paths.get(copyToPath, path.getFileName.toString)
            // noinspection SimplifyBooleanMatch
            if (Files.isDirectory(path)) {
                FileUtils.copyDirectory(path.toFile, destination.toFile)
            } else if (path.toString != destination.toString) {
                FileUtils.copyFile(path.toFile, destination.toFile)
            }
        }
    }

}
