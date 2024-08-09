package com.databricks.labs.mosaic.utils

import com.databricks.labs.mosaic.{NO_DRIVER, NO_PATH_STRING}
import com.databricks.labs.mosaic.core.raster.api.FormatLookup
import com.databricks.labs.mosaic.core.raster.io.RasterIO
import com.databricks.labs.mosaic.functions.{ExprConfig, MosaicContext}

import java.nio.file.{Files, Path, Paths}
import java.util.{Locale, UUID}
import scala.jdk.CollectionConverters._
import scala.util.Try

object PathUtils {

    val FILE_TOKEN = "file:"
    val VSI_ZIP_TOKEN = "/vsizip/"
    val DBFS_FUSE_TOKEN = "/dbfs"
    val DBFS_TOKEN = "dbfs:"
    val VOLUMES_TOKEN = "/Volumes"
    val WORKSPACE_TOKEN = "/Workspace"

    val FS_URI_TOKENS = Seq(FILE_TOKEN, DBFS_TOKEN)

    /**
     * For clarity, this is the function to call when you want a path that could actually be found on the file system.
     * - calls `getCleanPath` with 'addVsiZipToken' set to false.
     * - handle fuse conversion
     * - also, strips all uris if detected
     * - non guarantees the path actually exists.
     *
     * @param rawPath
     *   Path to clean for file system.
     * @param uriGdalOpt
     *   Option uri part.
     * @return
     *   Cleaned path.
     */
    def asFileSystemPath(rawPath: String, uriGdalOpt: Option[String]): String = {
        val cleanPath = getCleanPath(rawPath, addVsiZipToken = false, uriGdalOpt)
        PathUtils.stripGdalUriPart(cleanPath, uriGdalOpt)
    }

    /**
     * See asFileSystemPath.
     * - difference is that null or [[NO_PATH_STRING]] return None.
     *
     * @param rawPath
     *   Path to clean for file system.
     * @param uriGdalOpt
     *   Option string to override path, default is None
     * @return
     *   Option string.
     */
    def asFileSystemPathOpt(rawPath: String, uriGdalOpt: Option[String]): Option[String] = {
        if (rawPath == null || rawPath == NO_PATH_STRING) None
        else Option(asFileSystemPath(rawPath, uriGdalOpt))
    }

    //scalastyle:off println
    /**
     * Get subdataset GDAL path.
     * - these raw paths end with ":subdataset".
     * - handle 'file:' and 'dbfs:' and call [[prepFusePath]].
     * - adds "/vsizip/" if needed.
     * - strips quotes.
     * - converts ".zip:" to ".zip[/...] for relative paths.
     *
     * @param rawPath
     *   Provided path.
     * @param uriGdalOpt
     *   Option uri part.
     * @return
     *   Standardized path.
     */
    def asSubdatasetGdalPathOpt(rawPath: String, uriGdalOpt: Option[String]): Option[String] =
        Try {
            // Subdatasets are paths with a colon in them.
            // We need to check for this condition and handle it.
            // Subdatasets paths are formatted as: "FORMAT:/path/to/file.tif:subdataset"

            // (1) To avoid confusion, we want to handle fuse uri first.
            // - removing '"' (quotes)
            // - stripping [[VSI_ZIP_TOKEN]] at the start
            // - also, no [[FS_URI_TOKENS]] will be present
            val rawPathMod = this.prepFusePath(
                rawPath,
                keepVsiZipToken = false
            )

            var isZip = false // <- will be updated
            val result = {
                if (!isSubdataset(rawPathMod, uriGdalOpt)) {
                    // (3) not a sub path
                    //println(s"PathUtils - asSubdatasetGdalPathOpt - rawPathMod '$rawPathMod' not a subdataset")
                    None
                } else {
                    // (4) is a sub path
                    //println(s"PathUtils - asSubdatasetGdalPathOpt - rawPathMod '$rawPathMod' is a subdataset")
                    val subTokens = getSubdatasetTokenList(rawPathMod)
                    if (uriGdalOpt.isDefined && subTokens.length == 3) {
                        // (4a) 3 tokens
                        val uriSchema :: filePath :: subdataset :: Nil = subTokens
                        isZip = filePath.endsWith(".zip")
                        val subPath = {
                            if (isZip) {
                                // (4a1) handle zip
                                // - note the change to '.zip/<subdataset>' (instead of '.zip:')
                                // - note the addition of [[VSI_ZIP_TOKEN]]
                                // - note the dropping of the `uriSchema`
                                val subsetToken = {
                                    if (subdataset.startsWith("/")) subdataset
                                    else s"/$subdataset"
                                }
                                s"$VSI_ZIP_TOKEN$filePath$subsetToken"
                            } else {
                                // (4a2) essentially provide back `rawPathMod`
                                s"$uriSchema:$filePath:$subdataset"
                            }
                        }
                        //println(s"PathUtils - asSubdatasetGdalPathOpt - subPath (parsed from 3 tokens)? '$subPath'")
                        Some(subPath)
                    } else {
                        // (4b) assumed 2 tokens (since is a subdataset)
                        val filePath :: subdataset :: Nil = subTokens
                        isZip = filePath.endsWith(".zip")
                        val subPath = {
                            if (isZip) {
                                // (4b1) handle zip
                                // - note the change to '.zip/<subdataset>' (instead of '.zip:')
                                // - note the addition of [[VSI_ZIP_TOKEN]]
                                s"$VSI_ZIP_TOKEN$filePath/$subdataset"
                            } else {
                                // (4b2) handle non-zip
                                // - note the attempt to add back a URI from the driver name
                                val extOpt = this.getExtOptFromPath(filePath, uriGdalOpt = None)
                                val extDriverName = RasterIO.identifyDriverNameFromExtOpt(extOpt)
                                val uriSchema = if (extDriverName != NO_DRIVER) s"${extDriverName.toUpperCase(Locale.ROOT)}:" else ""
                                s"$uriSchema$filePath:$subdataset"
                            }
                        }
                        //println(s"PathUtils - asSubdatasetGdalPathOpt - subPath (parsed from 2 tokens)? '$subPath'")
                        Some(subPath)
                    }
                }
            }
            result
        }.getOrElse(None)
    //scalastyle:on println

    /**
     * Get GDAL path.
     * - handles with/without subdataset.
     * - handle 'file:' and 'dbfs:' and call [[prepFusePath]].
     * - adds "/vsizip/" if needed.
     * - strips quotes.
     * - converts ".zip:" to ".zip[/...] for relative paths.
     *
     * @param rawPath
     *   Raw path to clean-up.
     * @param uriGdalOpt
     *   Option uri part.
     * @return
     */
    def asGdalPathOpt(rawPath: String, uriGdalOpt: Option[String]): Option[String] = {
        PathUtils.asSubdatasetGdalPathOpt(rawPath, uriGdalOpt) match {
            case Some(sp) =>
                // (1) try for subdataset path first
                // - keeps uri unless zip
                Some(sp)
            case _ =>
                // (2) if not successful, go for clean path (all but subdataset portion)
                // - keeps uri unless zip
                Some(PathUtils.getCleanPath(rawPath, addVsiZipToken = true, uriGdalOpt))
        }
    }

    /**
      * Cleans up variations of path.
      * - 0.4.3 recommend to let CleanUpManager handle local files based on
      *   a session configured file / dir modification age-off policy.
      * - handles subdataset path
      * - handles "aux.xml" sidecar file
      * - handles zips, including "/vsizip/"
      * @param rawPath
      *   Raw path to clean-up.
      * @param uriGdalOpt
      *   Option uri part.
      */
    @deprecated("0.4.3 recommend to let CleanUpManager handle")
    def cleanUpPath(rawPath: String, uriGdalOpt: Option[String]): Unit = {
        val cleanPath = getCleanPath(rawPath, addVsiZipToken = false, uriGdalOpt)
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
     *   Will list directories recursively, will get a subdataset path or a clean path otherwise.
     * @param uriGdalOpt
     *   Option uri part.
     */
    def cleanUpPAMFiles(rawPathOrDir: String, uriGdalOpt: Option[String]): Unit = {
        if (isSubdataset(rawPathOrDir, uriGdalOpt)) {
            // println(s"... subdataset path detected '$path'")
            Try(Files.deleteIfExists(
                Paths.get(s"${asFileSystemPath(rawPathOrDir, uriGdalOpt)}.aux.xml"))
            )
        } else {
            val cleanPathObj = Paths.get(getCleanPath(rawPathOrDir, addVsiZipToken = false, uriGdalOpt))
            if (Files.isDirectory(cleanPathObj)) {
                // println(s"... directory path detected '$cleanPathObj'")
                cleanPathObj.toFile.listFiles()
                    .filter(f => f.isDirectory || f.toString.endsWith(".aux.xml"))
                    .foreach(f => cleanUpPAMFiles(f.toString, uriGdalOpt))
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
      * @param dirOpt
      *   Option string to not have one generated, defaults to None.
      * @return
      *   The copied path.
      */
    def copyToTmp(inRawPath: String, exprConfigOpt: Option[ExprConfig], dirOpt: Option[String] = None): String = {
        val copyFromPath = prepFusePath(inRawPath, keepVsiZipToken = false)
        val inPathDir = Paths.get(copyFromPath).getParent.toString
        val fullFileName = copyFromPath.split("/").last
        val stemRegexOpt = Option(getStemRegex(inRawPath))
//        scalastyle:off println
//        println(s"... `copyToTmp` copyFromPath? '$copyFromPath', inPathDir? '$inPathDir', " +
//            s"fullFileName? '$fullFileName', stemRegex? '$stemRegex'")
//        scalastyle:on println
        val toDir = dirOpt match {
            case Some(dir) => dir
            case _ => MosaicContext.createTmpContextDir(exprConfigOpt)
        }
        wildcardCopy(inPathDir, toDir, stemRegexOpt)

        s"$toDir/$fullFileName"
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
        val tmpDirOpt = Option(MosaicContext.createTmpContextDir(exprConfigOpt))
        var tmpPath = copyToTmp(inCleanPath, exprConfigOpt, dirOpt = tmpDirOpt)
        var i = 0
        while (Files.notExists(Paths.get(tmpPath)) && i < retries) {
            tmpPath = copyToTmp(inCleanPath, exprConfigOpt, dirOpt = tmpDirOpt)
            i += 1
        }
        tmpPath
    }

    /**
      * Create a file under tmp dir.
      * - Directories are created.
      * - File itself is not create.
 *
      * @param ext
      * The file extension to use.
      * @param exprConfigOpt
      *   Option [[ExprConfig]]
      * @return
      *   The tmp path.
      */
    def createTmpFilePath(ext: String, exprConfigOpt: Option[ExprConfig]): String = {
        val tmpDir = MosaicContext.createTmpContextDir(exprConfigOpt)
        val filename = this.genFilenameUUID(ext, uuidOpt = None)
        val outPath = s"$tmpDir/$filename"
        Files.createDirectories(Paths.get(outPath).getParent)
        outPath
    }

    /** @return UUID standardized for use in Path or Directory. */
    def genUUID: String = UUID.randomUUID().toString.replace("-", "_")

    /** @return filename with UUID standardized for use in Path or Directory (raster_<uuid>.<ext>). */
    def genFilenameUUID(ext: String, uuidOpt: Option[String]): String = {
        val uuid = uuidOpt match {
            case Some(u) => u
            case _ => genUUID
        }
        s"raster_$uuid.$ext"
    }

    /** @return Returns file extension as option (path converted to clean path). */
    def getExtOptFromPath(path: String, uriGdalOpt: Option[String]): Option[String] =
        Try {
            val ext = Paths.get(asFileSystemPathOpt(path, uriGdalOpt).orNull)
                .getFileName.toString
                .split("\\.").last
            Some(ext)
        }.getOrElse(None)

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
        val cleanPath = prepFusePath(path, keepVsiZipToken = false)
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
     * @param uriGdalOpt
     *   Option uri part.
     * @return
     *   Option subdatasetName
     */
    def getSubdatasetNameOpt(rawPath: String, uriGdalOpt: Option[String]): Option[String] =
        Try {
            // Subdatasets are paths with a colon in them.
            // We need to check for this condition and handle it.
            // Subdatasets paths are formatted as: "FORMAT:/path/to/file.tif:subdataset"
            val subTokens = getSubdatasetTokenList(rawPath)
            val result = {
                if (subTokens.length == 3) {
                    val _ :: _ :: subdataset :: Nil = subTokens
                    Some(subdataset) // <- uri with a sub
                } else if (subTokens.length == 2 && uriGdalOpt.isEmpty) {
                    val t1 :: t2 :: Nil = subTokens
                    Some(t2) // <- no uri so have a sub
                } else {
                    None
                }
            }
            result
        }.getOrElse(None)

    /**
     * Get Subdataset Tokens
     * - This is to enforce convention.
     * - converts '.zip/' to '.zip:'
     * - fuse conversion for consistency.
     *
     * @param rawPath
     *   To split into tokens (based on ':').
     * @return
     *   [[List]] of string tokens from the path.
     */
    def getSubdatasetTokenList(rawPath: String): List[String] = {
        // !!! avoid cyclic dependencies !!!
        Try {
            this.prepFusePath(rawPath, keepVsiZipToken = true).split(":").toList
        }.getOrElse(List.empty[String])
    }

    /**
      * Clean file path. This is different from `asFileSystemPath` as it is more GDAL friendly,
      * effectively strips subdataset portion but otherwise looks like a GDAL path:
      *
      * (1) ':subdataset' (may be zips)
      *     also '.zip/' and '.zip:'
      * (2) "normal" zips
      * (3) handle fuse ready
      * (4) handles zips for vsizip + uri
      *
      * @param rawPath
      *   Provided path.
      * @param addVsiZipToken
      *   Specify whether the result should include [[VSI_ZIP_TOKEN]].
      * @param uriGdalOpt
      *   Option uri part.
      * @return
      *   Standardized file path string.
      */
    def getCleanPath(rawPath: String, addVsiZipToken: Boolean, uriGdalOpt: Option[String]): String = {

        val result = {
            if (isSubdataset(rawPath, uriGdalOpt)) {
                // (1) GDAL path for subdataset - without name
                //println(s"PathUtils - getCleanPath -> getWithoutSubdatasetName for rawPath '$rawPath'")
                getWithoutSubdatasetName(rawPath, addVsiZipToken, uriGdalOpt)
            } else if (rawPath.endsWith(".zip")) {
                // (2a) normal zip (not a subdataset)
                // - initially remove the [[VSI_ZIP_TOKEN]]
                var result = this.prepFusePath(rawPath, keepVsiZipToken = false)

                // (2b) for zips, take out the GDAL uri (if any)
                result = this.stripGdalUriPart(result, uriGdalOpt)

                // (2c) if 'addVsiZipToken' true, add [[VSI_ZIP_TOKEN]] to zips; conversely, remove if false
                // - It is really important that the resulting path is /vsizip// and not /vsizip/
                // /vsizip// is for absolute paths /viszip/ is relative to the current working directory
                // /vsizip/ wont work on a cluster.
                // - See: https://gdal.org/user/virtual_file_systems.html#vsizip-zip-archives
                if (addVsiZipToken && !this.hasVisZipToken(result)) {
                    // (2d) final condition where "normal" zip still hasn't had the [[VSI_ZIP_TOKEN]] added
                    result = s"$VSI_ZIP_TOKEN$result"
                } else if (!addVsiZipToken) {
                    // (2e) final condition to strip [[VSI_ZIP_TOKEN]]
                    result = this.replaceVsiZipToken(result)
                }

                result
            } else {
                // (3) just prep for fuse (not a zip)
                this.prepFusePath(rawPath, keepVsiZipToken = false)
            }
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
      * @param uriGdalOpt
      *   Option uri part.
      * @return
      *   True if path is in a fuse location.
      */
    def isFusePathOrDir(path: String, uriGdalOpt: Option[String]): Boolean = {
        // clean path strips out "file:" and "dbfs:".
        // also, strips out [[VSI_ZIP_TOKEN]].
        // then can test for start of the actual file path,
        // startswith [[DBFS_FUSE_TOKEN]], [[VOLUMES_TOKEN]], or [[WORKSPACE_TOKEN]].
        // 0.4.3 - new function
        getCleanPath(path, addVsiZipToken = false, uriGdalOpt) match {
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
      * - Handles '.zip/' as '.zip:'
      * - 0.4.3+  `isURIPath` to know if expecting 1 or 2 ":" in path.
      *
      * @param rawPath
      *   Provided path.
      * @param uriGdalOpt
      *   Option uri part.
      * @return
      *   True if is a subdataset.
      */
    def isSubdataset(rawPath: String, uriGdalOpt: Option[String]): Boolean = {
        val subTokens = getSubdatasetTokenList(rawPath)
        if (subTokens.length == 3) true  // <- uri token assumed
        else if (uriGdalOpt.isEmpty && subTokens.length == 2) true // <- no uri token
        else false
    }

    /**
     * Test if a path is a zip file.
     * - Don't call this within `getCleanPath` and similar.
     *
     * @param rawPath
     *   The path to test (doesn't have to be raw).
     * @param uriGdalOpt
     *   Option uri part.
     * @return
     *   Whether file system path portion ends with ".zip".
     */
    def isZip(rawPath: String, uriGdalOpt: Option[String]): Boolean = {
        this.asFileSystemPath(rawPath, uriGdalOpt).endsWith(".zip")
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
     * For GDAL URIs, e.g. 'ZARR', 'NETCDF', 'COG', 'GTIFF', and 'GRIB':
     * - Call `parseGdalUriOpt` for the actual detected token.
     * - Not for file system URIs, i.e. 'file:' or 'dbfs:'.
     *
     * @param rawPath
     *   To check.
     * @param uriDeepCheck
     *   Whether to do a deep check of URIs or just more common ones.
     * @return
     *   Whether a uri token detected.
     */
    def hasGdalUriPart(
                  rawPath: String,
                  uriDeepCheck: Boolean
              ): Boolean = {
        this.parseGdalUriOpt(
            rawPath,
            uriDeepCheck
        ).isDefined
    }

    //scalastyle:off println
    /**
     * For GDAL URIs, e.g. 'ZARR', 'NETCDF', 'COG', 'GTIFF', and 'GRIB':
     * - Not for file system URIs, i.e. 'file:' or 'dbfs:'.
     *
     * @param rawPath
     *   To check.
     * @param uriDeepCheck
     *   Whether to do a deep check of URIs or just more common ones.
     * @return
     *   Option with a matched token, must be in one of the lists under `FormatLookup` to be detected.
     */
    def parseGdalUriOpt(
                       rawPath: String,
                       uriDeepCheck: Boolean
                   ): Option[String] = Try {

        var uriOpt: Option[String] = None
        var t1: String = ""
        var t1Low: String = ""

        // (1) split on ":"
        val subTokens = this.getSubdatasetTokenList(rawPath)

        if (subTokens.length > 1) {
            // (2) nothing to do if < 2 tokens
            // - standardize raw path
            t1 = subTokens.head.replace(VSI_ZIP_TOKEN, "") // <- no colon here
            t1Low = subTokens.head.toLowerCase(Locale.ROOT).replace(VSI_ZIP_TOKEN, "") + ":"

           if (FormatLookup.COMMON_URI_TOKENS.exists(k => t1Low.startsWith(k.toLowerCase(Locale.ROOT)))) {
                // (4) check [[COMMON_URI_TOKENS]]
                uriOpt = Option(t1)
            } else if (uriDeepCheck) {
                if (FormatLookup.formats.keys.exists(k => t1Low.startsWith(s"${k.toLowerCase(Locale.ROOT)}:"))) {
                    // (5a) Deep Check `formats` keys (have to add ':')
                    uriOpt = Option(t1)
                } else if (FormatLookup.ALL_VECTOR_URI_TOKENS.exists(k => t1Low.startsWith(k.toLowerCase(Locale.ROOT)))) {
                    // (5b) Deep Check [[ALL_VECTOR_URI_TOKENS]]
                    uriOpt = Option(t1)
                } else if (FormatLookup.ALL_RASTER_URI_TOKENS.exists(k => t1Low.startsWith(k.toLowerCase(Locale.ROOT)))) {
                    // (5b) Deep Check [[ALL_RASTER_URI_TOKENS]]
                    uriOpt = Option(t1)
                }
            }
        }
            uriOpt
        }.getOrElse(None)

    /**
     *  Strip the uri part out of the rawPath, if found.
     *  - You would want to handle fuse before calling this!
     *  - handles with and without colon
     *  - not case sensitive (use the `parseGdalUriOpt` function to get the right case).
     *  - careful calling this on a subdataset path, e.g. don't want to strip ".zip:" to be "."
     * @param rawPath
     *   To check.
     * @param uriGdalOpt
     *   Option with uri part, if any.
     * @return
     *   path with the uri part stripped out.
     */
    def stripGdalUriPart(rawPath: String, uriGdalOpt: Option[String]): String = {
        uriGdalOpt match {
            case Some(uriPart) =>
                val uri = {
                    if (uriPart.endsWith(":")) uriPart
                    else s"$uriPart:"
                }
                rawPath.replace(s"$uri", "")
            case _ => rawPath
        }
    }

    //scalastyle:off println
    /**
      * Perform a wildcard copy.
      * - This is pure file system based operation,
      *   with some regex around the 'pattern'.
      *
      * @param inDirPath
      *   Provided in dir.
      * @param outDirPath
      *   Provided out dir.
      * @param patternOpt
      *   Option, regex pattern to match, will default to ".*"
      */
    def wildcardCopy(inDirPath: String, outDirPath: String, patternOpt: Option[String]): Unit = {
        //println(s"::: wildcardCopy :::")
        import org.apache.commons.io.FileUtils
        val copyFromPath = prepFusePath(inDirPath, keepVsiZipToken = false)
        val copyToPath = prepFusePath(outDirPath, keepVsiZipToken = false)

        val pattern = patternOpt.getOrElse(".*")
        //println(s"... from: '$copyFromPath', to: '$copyToPath' (pattern '$pattern')")

        val toCopy = Files
            .list(Paths.get(copyFromPath))
            .filter(_.getFileName.toString.matches(pattern))
            .collect(java.util.stream.Collectors.toList[Path])
            .asScala

        for (path: Path <- toCopy) {
            val destination = Paths.get(copyToPath, path.getFileName.toString)
            // noinspection SimplifyBooleanMatch
            if (Files.isDirectory(path)) {
                //println(s"...path '${path.toString}' is directory (copying dir)")
                org.apache.commons.io.FileUtils.copyDirectory(path.toFile, destination.toFile)
            } else if (path.toString != destination.toString) {
                //println(s"... copying '${path.toString}' to '${destination.toString}'")
                Files.copy(path, destination)
            } else {
                //println(s"INFO - dest: '${destination.toString}' is the same as path (no action)")
            }
        }
    }
    //scalastyle:on println

    /** private for handling needed by other functions in PathUtils only. */
    private def getWithoutSubdatasetName(
                                            rawPath: String,
                                            addVsiZipToken: Boolean,
                                            uriGdalOpt: Option[String]
                                        ): String = {

        // (1) Subdatasets are paths with a colon in them.
        // We need to check for this condition and handle it.
        // Subdatasets paths are formatted as: "FORMAT:/path/to/file.tif:subdataset"
        // Additionally if the path is a zip, the format looks like "FORMAT:/vsizip//path/to/file.zip:subdataset"
        var isZip = false // <- set in the logic below
        val tokens = getSubdatasetTokenList(rawPath)
        var result = {
            if (tokens.length == 3) {
                // first and second token returned (not subdataset name)
                val uriSchema :: filePath :: _ :: Nil = tokens
                isZip = filePath.endsWith(".zip")
                if (isZip) filePath
                else s"$uriSchema:$filePath"
            } else if (uriGdalOpt.isDefined && tokens.length == 2) {
                // uri detected + filepath
                val uriSchema :: filePath :: Nil = tokens
                isZip = filePath.endsWith(".zip")
                if (isZip) filePath
                else s"$uriSchema:$filePath"
            } else if (tokens.length == 2) {
                // no uri detected, only return the first token
                val filePath :: _ :: Nil = tokens
                isZip = filePath.endsWith(".zip")
                filePath
            } else {
                // return rawPath prepped
                this.prepFusePath(rawPath, keepVsiZipToken = addVsiZipToken)
            }
        }
        // (2)handle vsizip
        // - add for zips if directed
        // - remove for all if directed
        if (isZip && addVsiZipToken && !result.startsWith(VSI_ZIP_TOKEN)) result = s"$VSI_ZIP_TOKEN$result"
        if (!addVsiZipToken) result = this.replaceVsiZipToken(result)

        result
    }

    /** private, handle file system uris for local fuse, also call `prepPath`. */
    def prepFusePath(rawPath: String, keepVsiZipToken: Boolean): String = {
        // !!! avoid cyclic dependencies !!!
        val rawPathMod = this.prepPath(rawPath)
        // (1) does the path have [[VSI_ZIP_TOKEN]]?
        val hasVsi = this.hasVisZipToken(rawPathMod)
        // (2) remove [[VSI_ZIP_TOKEN]] and handle fuse tokens
        var result = replaceVsiZipToken(rawPathMod)
            .replace(s"$FILE_TOKEN/", "/")
            .replace(s"$DBFS_TOKEN$VOLUMES_TOKEN/", s"$VOLUMES_TOKEN/")
            .replace(s"$DBFS_TOKEN/", s"$DBFS_FUSE_TOKEN/")
        // (3) if conditions met, prepend [[VSI_ZIP_TOKEN]]
        if (hasVsi && keepVsiZipToken) {
            result = s"$VSI_ZIP_TOKEN$result"
        }
        result
    }

    private def prepPath(path: String): String = {
        // !!! avoid cyclic dependencies !!!
        // (1) null
        var p = if (path == null) NO_PATH_STRING else path
        // (2) quotes
        p = p.replace("\"", "")
        // (3) '.zip/' for non-directories
        // - for subdataset path initial inputs
        // - will end up as ".zip/" during subdataset processing
        val pPath = Paths.get(p)
        if (!Files.exists(pPath) || !Files.isDirectory(pPath)) {
            // if not a real file or is but not a directory
            // strip trailing '/' for '.zip/'
            // replace '.zip/' with '.zip:'
            if (p.endsWith(".zip/")) p = p.dropRight(1)
            p = p.replace(".zip/", ".zip:")
        }

        p
    }

}
