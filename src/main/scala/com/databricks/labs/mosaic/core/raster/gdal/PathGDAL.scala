package com.databricks.labs.mosaic.core.raster.gdal

import com.databricks.labs.mosaic.NO_PATH_STRING
import com.databricks.labs.mosaic.utils.PathUtils

import java.nio.file.{Files, Paths}
import scala.util.Try

/**
  * 'path' is the only variable updated / set on this object.
  *   - everything else is derived from 'path'.
  *   - 'path' is a var, meaning it can be updated.
  *   - 'path' defaults to [[NO_PATH_STRING]]
  *
  * @param path
  */
case class PathGDAL(var path: String = NO_PATH_STRING) {

    // /////////////////////////////////////////////////////////////
    // FUNCTIONS FOR PATH
    // /////////////////////////////////////////////////////////////

    def asFileSystemPath: String = PathUtils.asFileSystemPath(path)

    def asFileSystemPathOpt: Option[String] = asFileSystemPath match {
        case p if p != NO_PATH_STRING => Option(p)
        case _ => None
    }

    def asSubdatasetGDALFuseOpt: Option[String] = PathUtils.asSubdatasetGDALPathOpt(path, uriFuseReady = true)

    /**
      * This is a check of the file
      * @return
      *   whether the path exists on the file system.
      */
    def existsOnFileSystem: Boolean = Try(Files.exists(Paths.get(asFileSystemPath))).getOrElse(false)

    /**
      * @return
      *   Returns file extension as option (path converted to file system path).
      */
    def getExtOpt: Option[String] = PathUtils.getExtOptFromPath(path)

    def getPathOpt: Option[String] = {
        if (path == NO_PATH_STRING) None
        else Option(path)
    }

    /** @return option for subdataset name. */
    def getSubdatasetNameOpt: Option[String] = PathUtils.getSubdatasetNameOpt(path)

    /** @return whether the path is (could be coerced to) a fuse path */
    def isFusePath: Boolean = PathUtils.isFusePathOrDir(path)

    /** @return whether the path option is defined. */
    def isPathSet: Boolean = getPathOpt.isDefined

    /**
      * @return
      *   whether the path option is defined and exists on the filesystem.
      */
    def isPathSetAndExists: Boolean = isPathSet && existsOnFileSystem

    /** @return whether pathutils ids the path as a subdataset. */
    def isSubdatasetPath: Boolean = PathUtils.isSubdataset(path)

    /** @return - set path back to [[NO_PATH_STRING]] and return `this` (fluent). */
    def resetPath: PathGDAL = {
        this.path = NO_PATH_STRING
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
        this
    }

    /**
     * Writes a raster to a specified file system path.
     *
     * @param toDir
     *   The path to write the raster.
     * @param doDestroy
     *   A boolean indicating if the raster object should be destroyed after
     *   writing.
     *   - file paths handled separately.
     * @return
     *   The path where written (may differ, e.g. due to subdatasets).
     */
    def rawPathWildcardCopyToDir(toDir: String, doDestroy: Boolean): Option[String] = {
            this.asFileSystemPathOpt match {
                case Some(fsPath) =>
                    // (1) paths
                    val thisJavaPath = Paths.get(fsPath)
                    val rasterFileName = thisJavaPath.getFileName.toString
                    val rasterDir = thisJavaPath.getParent.toString
                    val toPath = s"$toDir/$rasterFileName"

                    // (2) copy all files with same stem from raster dir to new dir
                    // - this will handle sidecar files and such
                    val stemRegex = PathUtils.getStemRegex(this.path)
                    PathUtils.wildcardCopy(rasterDir, toDir, stemRegex)

                    Option(toPath)
                case _ => None
            }
        }

}
