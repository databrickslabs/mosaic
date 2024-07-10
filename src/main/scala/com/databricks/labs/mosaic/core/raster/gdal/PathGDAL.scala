package com.databricks.labs.mosaic.core.raster.gdal

import com.databricks.labs.mosaic.{NO_DRIVER, NO_PATH_STRING}
import com.databricks.labs.mosaic.core.raster.io.RasterIO
import com.databricks.labs.mosaic.utils.{PathUtils, SysUtils}

import java.nio.file.{Files, Path, Paths}
import scala.util.Try

/**
  * 'path' is the only variable updated / set on this object.
  *   - everything else is derived from 'path'.
  *   - 'path' is a var, meaning it can be updated.
  *   - 'path' defaults to [[NO_PATH_STRING]].
  *   - 'uriDeepCheck' defaults to false.
  * @param path
  * @param uriDeepCheck
  */
case class PathGDAL(var path: String = NO_PATH_STRING, var uriDeepCheck: Boolean = false) {

    // these are parsed 1x on init, and as needed after.
    // and then only as path changes,
    // since they are more expensive (and can be repetitive)
    private var isFuse: Boolean = false
    private var driverNameOpt: Option[String] = None
    private var extOpt: Option[String] = None
    private var fsPathOpt: Option[String] = None
    private var gdalPathOpt: Option[String] = None
    private var subNameOpt: Option[String] = None
    private var uriGdalOpt: Option[String] = None

    // track when refresh is needed
    // - doubles as an init flag
    private var refreshFlag: Boolean = true
    this.refreshParts() // <- go ahead and force refresh

    // /////////////////////////////////////////////////////////////
    // FILE SYSTEM FUNCTIONS FOR PATH
    // - These are for the base file only, not subdatasets
    // /////////////////////////////////////////////////////////////

    // `asFilSystem*` and `asJava*` functions are just for the base file (not subdataset, not for GDAL)
    def asFileSystemPath: String = this.asFileSystemPathOpt.getOrElse(NO_PATH_STRING)

    def asFileSystemPathOpt: Option[String] = {
        this.refreshParts()
        fsPathOpt
    }

    def asJavaPath: Path = Paths.get(this.asFileSystemPath)

    /**
     * This is a check of the file
     * @return
     *   whether the path exists on the file system.
     */
    def existsOnFileSystem: Boolean = Try(Files.exists(this.asJavaPath)).getOrElse(false)

    /**
     * @return
     *   Returns file extension as option (path converted to file system path).
     */
    def getExtOpt: Option[String] = {
        this.refreshParts()
        extOpt
    }

    /** @return the filename from the filesystem */
    def getFilename: String = this.asJavaPath.getFileName.toString

    /** @return the parsed uriGdalOpt, if any. */
    def getUriGdalOpt: Option[String] = {
        this.refreshParts()
        uriGdalOpt
    }

    /** @return whether the file system path is a directory. */
    def isDir: Boolean = Try(Files.isDirectory(this.asJavaPath)).getOrElse(false)

    /** @return whether the path is (could be coerced to) a fuse path */
    def isFusePath: Boolean = {
        this.refreshParts()
        isFuse
    }

    /** @return whether the path is a zip (from file system path check). */
    def isPathZip: Boolean = {
        this.refreshParts()
        fsPathOpt match {
            case Some(fsPath) => fsPath.endsWith(".zip")
            case _ => false
        }
    }

    // //////////////////////////////////////////////////////////////
    // GDAL PATH FUNCTIONS
    // - These are for loading raw path with GDAL,
    //   including subdatasets
    // //////////////////////////////////////////////////////////////

    def asGDALPathOpt: Option[String] = {
        this.refreshParts()
        gdalPathOpt
    }

    /** @return a driver if known from path extension, default [[NO_DRIVER]]. */
    def getPathDriverName: String = {
        this.refreshParts()
        driverNameOpt match {
            case Some(d) => d
            case _ => NO_DRIVER
        }
    }

    /** @return a driver option, not allowing [[NO_DRIVER]]. */
    def getPathDriverNameOpt: Option[String] = {
        this.refreshParts()
        driverNameOpt
    }

    def hasPathDriverName: Boolean = {
        this.refreshParts()
        driverNameOpt.isDefined
    }

    /** @return option for subdataset name. */
    def getPathSubdatasetNameOpt: Option[String] = {
        this.refreshParts()
        subNameOpt
    }

    /** @return whether pathutils ids the path as a subdataset. */
    def isSubdatasetPath: Boolean = {
        this.refreshParts()
        subNameOpt.isDefined
    }

    // //////////////////////////////////////////////////////////////
    // ADDITIONAL PATH FUNCTIONS
    // //////////////////////////////////////////////////////////////

    /** @return None if path [[NO_PATH_STRING]]. */
    def getPathOpt: Option[String] = {
        if (path == NO_PATH_STRING) None
        else Option(path)
    }

    /** @return whether the path option is defined. */
    def isPathSet: Boolean = this.getPathOpt.isDefined

    /**
      * @return
      *   whether the path option is defined and exists on the filesystem.
      */
    def isPathSetAndExists: Boolean = this.isPathSet && this.existsOnFileSystem

    //scalastyle:off println
    /**
     * Writes a tile to a specified file system path.
     *
     * @param toDir
     *   The path to write the tile.
     * @param skipUpdatePath
     *   Whether to update the path on [[PathGDAL]].
     * @return
     *   Option string for where the main path was written (may include additional files).
     */
    def rawPathWildcardCopyToDir(toDir: String, skipUpdatePath: Boolean): Option[String] =
        Try {
            Files.createDirectories(Paths.get(toDir)) // <- ok exists
            //println("::: PathGDAL - rawPathWildcardCopyToDir :::")
            val thisDir = this.asJavaPath.getParent.toString
            val thisFN = this.getFilename
            //println(s"isDir? ${this.isDir}")
            val outPathOpt: Option[String] = this.asFileSystemPathOpt match {
                case Some(_) if !this.isDir =>
                    // (1a) wildcard copy based on filename
                    // - this is the path returned
                    val toPath = s"$toDir/$thisFN"

                    // (1b) copy all files with same stem from tile dir to new dir
                    // - this will handle sidecar files and such
                    // - use the raw path here
                    val stemRegexOpt = Option(PathUtils.getStemRegex(this.asFileSystemPath))
                    PathUtils.wildcardCopy(thisDir, toDir, stemRegexOpt)

                    Option(toPath)
                case Some(_) if this.isDir =>
                    // (2a) for a directory (vs file path), zip it up
                    // - requires `zip` native installed
                    val prompt = SysUtils.runScript(Array("/bin/sh", "-c", s"cd $thisDir && zip -r0 $thisFN.zip $thisFN"))
                    if (prompt._3.nonEmpty) {
                        throw new Exception(
                            s"Error zipping file: ${prompt._3}. Please verify that zip is installed. " +
                                s"Run `apt install zip`."
                        )
                    }
                    val fromZip = s"$thisDir/$thisFN.zip"
                    val toZip = s"$toDir/$thisFN.zip"
                    Files.move(Paths.get(fromZip), Paths.get(toZip))

                    // (2b) return the path to the zip
                    Option(toZip)
                case _ =>
                    // (3) not a valid filesystem path, e.g. [[NO_PATH_STRING]]
                    //println(s"PathGDAL - path: '$path' not filesystem path?")
                    None
            }

            if (!skipUpdatePath) {
                outPathOpt match {
                    case Some(outPath) => this.updatePath(outPath)
                    case _ => this.updatePath(NO_PATH_STRING)
                }
            }

            outPathOpt
        }.getOrElse {
            // (4) unable to act on the file, does it exist?
            //println(s"PathGDAL - Exception - does raw path: '$path' exist?")
            None
        }
    //scalastyle:on println

    /**
     * Refresh the various parts of the path.
     * - This is to avoid recalculating, except when path changes.
     *
     * @param forceRefresh
     *   Whether to force the refresh.
     * @return
     *   [[PathGDAL]] this (fluent).
     */
    def refreshParts(forceRefresh: Boolean = false): PathGDAL = {
        try {
            if (refreshFlag || forceRefresh) {
                // work from `getPathOpt` (ok to call)
                getPathOpt match {
                    case Some(p) =>
                        // handle `uriGdalOpt` first
                        // - then pass it to others to avoid recompute
                        uriGdalOpt = PathUtils.parseGdalUriOpt(p, this.uriDeepCheck)
                        extOpt = PathUtils.getExtOptFromPath(p, uriGdalOpt)
                        driverNameOpt =  RasterIO.identifyDriverNameFromExtOpt(extOpt) match {
                            case d if d != NO_DRIVER => Some(d)
                            case _ => None
                        }
                        fsPathOpt = PathUtils.asFileSystemPathOpt(p, uriGdalOpt)
                        gdalPathOpt = PathUtils.asGdalPathOpt(p, uriGdalOpt)
                        isFuse = fsPathOpt match {
                            case Some(fsPath) => PathUtils.isFusePathOrDir(fsPath, uriGdalOpt)
                            case _ => false
                        }
                        subNameOpt = PathUtils.getSubdatasetNameOpt(p, uriGdalOpt)
                    case _ =>
                        // all get reset
                        isFuse = false
                        driverNameOpt = None
                        extOpt = None
                        fsPathOpt = None
                        gdalPathOpt = None
                        subNameOpt = None
                        uriGdalOpt = None
                }
            }
            this // <- fluent
        } finally {
            refreshFlag = false
        }
    }

    /** @return - set path back to [[NO_PATH_STRING]] and return `this` (fluent). */
    def resetPath: PathGDAL = {
        this.path = NO_PATH_STRING
        this.refreshParts(forceRefresh = true)
        this
    }

    /**
     * Set the object's path.
     *
     * @param path
     *   To set.
     * @return
     *   `this` [[PathGDAL]] (fluent).
     */
    def updatePath(path: String): PathGDAL = {
        this.path = path
        this.refreshParts(forceRefresh = true)
        this
    }

}
