package com.databricks.labs.mosaic.utils

import com.databricks.labs.mosaic.{MOSAIC_RASTER_TMP_PREFIX_DEFAULT, RASTER_PATH_KEY}
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.io.CleanUpManager
import com.databricks.labs.mosaic.gdal.MosaicGDAL
import com.databricks.labs.mosaic.utils.PathUtils.{DBFS_FUSE_TOKEN, VOLUMES_TOKEN, WORKSPACE_TOKEN}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.{col, explode}

import java.io.{BufferedInputStream, File, FileInputStream, IOException}
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, Paths, SimpleFileVisitor}
import java.util.concurrent.atomic.AtomicInteger
import scala.sys.process._
import scala.util.Try

object FileUtils {

    val MINUTE_IN_MILLIS = 60 * 1000

    /**
     * Read bytes from path.
     * @param path
     * @param uriDeepCheck
     * @return
     */
    def readBytes(path: String, uriDeepCheck: Boolean): Array[Byte] = {
        val bufferSize = 1024 * 1024 // 1MB
        val uriGdalOpt = PathUtils.parseGdalUriOpt(path, uriDeepCheck)
        val fsPath = PathUtils.asFileSystemPath(path, uriGdalOpt)
        val inputStream = new BufferedInputStream(new FileInputStream(fsPath))
        val buffer = new Array[Byte](bufferSize)

        var bytesRead = 0
        var bytes = Array.empty[Byte]

        while ({
            bytesRead = inputStream.read(buffer); bytesRead
        } != -1) {
            bytes = bytes ++ buffer.slice(0, bytesRead)
        }
        inputStream.close()
        bytes
    }

    /**
     * Create a temp dir using prefix for root dir, e.g. '/tmp'
     * @param prefix
     * @return
     */
    def createMosaicTmpDir(prefix: String = MOSAIC_RASTER_TMP_PREFIX_DEFAULT): String = {
        val tempRoot = Paths.get(prefix, "mosaic_tmp")
        if (!Files.exists(tempRoot)) {
            Files.createDirectories(tempRoot)
        }
        val tempDir = Files.createTempDirectory(tempRoot, "mosaic_")
        tempDir.toFile.getAbsolutePath
    }

    /** Delete provided path (only deletes empty dirs). */
    def tryDeleteFileOrDir(path: Path): Boolean = {
            if (!CleanUpManager.USE_SUDO) Try(Files.delete(path)).isSuccess
            else {
                val err = new StringBuilder()
                val procLogger = ProcessLogger(_ => (), err append _)
                val filePath = path.toString
                //scalastyle:off println
                //println(s"FileUtils - tryDeleteFileOrDir -> '$filePath'")
                //scalastyle:on println
                s"sudo rm -f $filePath" ! procLogger
                err.length() == 0
            }
    }

    /**
     * Consolidated logic to delete a local `fs` path:
     * - Used in `RasterGDAL.finalizeRaster` for StringType, `RasterTile.serialize` for BinaryType,
     *   and GDAL manipulating functions that generate a new local path (the old is removed).
     * - Since it is called a lot during processing, optimizing input requirements.
     * - Expects a file system path (no URIs), e.g. '/tmp/<some_path>/<some_file>'.
     * - For non-fuse locations only, it verifies, e.g. '/dbfs', '/Volumes', '/Workspace';
     *   does nothing if condition not met.
     * - For existing file paths only, it verifies; does nothing if condition not met.
     * - For directories that do not equal the current `MosaicGDAL.getLocalRasterDirThreadSafe` only,
     *   it verifies; does nothing if condition not met.
     * - deletes directory, even if not empty.
     * - deletes either a file or its parent directory (depending on `delParentFile`).
     *
     * @param fs
     *   The file system path to delete, should not include any URI parts and fuse already handled.
     * @param delParentIfFile
     *   Whether to delete the parent dir if a file.
     */
    def tryDeleteLocalFsPath(fs: String, delParentIfFile: Boolean): Unit = {
        val isFuse = fs match {
            case p if
                p.startsWith(s"$DBFS_FUSE_TOKEN/") ||
                    p.startsWith(s"$VOLUMES_TOKEN/") ||
                    p.startsWith(s"$WORKSPACE_TOKEN/") => true
            case _ => false
        }

        val fsPath = Paths.get(fs)
        if (
            !isFuse
                && Files.exists(fsPath)
                && fs != MosaicGDAL.getLocalRasterDirThreadSafe
        ) {
            if (Files.isDirectory(fsPath)) {
                deleteRecursively(fsPath, keepRoot = false)           // <- recurse the dir
                Try(Files.deleteIfExists(fsPath)) // <- delete the empty dir
            } else if (delParentIfFile) {
                deleteRecursively(fsPath.getParent, keepRoot = false) // <- recurse the parent dir
                Try(Files.deleteIfExists(fsPath))                     // <- delete the empty dir
                Try(Files.deleteIfExists(fsPath.getParent))           // <- delete the empty parent dir
            } else {
                Try(Files.deleteIfExists(fsPath))
            }
        }
    }

    /**
     * Delete files recursively (no conditions).
     *
     * @param root
     *   May be a directory or a file.
     * @param keepRoot
     *   If true, avoid deleting the root dir itself.
     */
    def deleteRecursively(root: Path, keepRoot: Boolean): Unit = {

        Files.walkFileTree(root, new SimpleFileVisitor[Path] {

            val handles = new AtomicInteger(0)

            override def visitFile(file: Path, attributes: BasicFileAttributes): FileVisitResult = {
                synchronized {
                    val numHandles = handles.incrementAndGet()
                    if (numHandles >= 100000) {
                        //scalastyle:off println
                        println(s"FileUtils - deleteRecursively -> attempting gc at next 100K+ handles detected ($numHandles) ...")
                        handles.set(0)
                        Try(System.gc())
                        println(s"FileUtils - deleteRecursively -> gc complete ...")
                        //scalastyle:on println
                    }
                }

                tryDeleteFileOrDir(file)
                FileVisitResult.CONTINUE
            }

            override def postVisitDirectory(dir: Path, exception: IOException): FileVisitResult = {
                if ((!keepRoot || dir.compareTo(root) != 0) && isEmptyDir(dir)) {
                    tryDeleteFileOrDir(dir)
                }
                FileVisitResult.CONTINUE
            }
        })
    }

    def deleteRecursively(root: File, keepRoot: Boolean): Unit = {
       deleteRecursively(root.toPath, keepRoot)
    }

    def deleteRecursively(path: String, keepRoot: Boolean): Unit = {
        deleteRecursively(Paths.get(path), keepRoot)
    }

    /**
     * Delete files recursively if they match the following age conditions:
     * - if < 0, e.g. -1 do not delete anything
     * - if 0 delete regardless of age
     * - if > 0 delete if the file last modified time is older than the age in minutes
     * @param root
     *   May be a directory or a file.
     * @param ageMinutes
     *   Age in minutes to test.
     * @param keepRoot
     *   If true, avoid deleting the root dir itself.
     */
    def deleteRecursivelyOlderThan(root: Path, ageMinutes: Int, keepRoot: Boolean): Unit = {
        if (ageMinutes == 0) {
            deleteRecursively(root, keepRoot)
        }
        else if (ageMinutes > 0 ) {
            val ageMillis = ageMinutes * MINUTE_IN_MILLIS

            Files.walkFileTree(root, new SimpleFileVisitor[Path] {

                val handles = new AtomicInteger(0)

                override def visitFile(file: Path, attributes: BasicFileAttributes): FileVisitResult = {
                    synchronized {
                        val numHandles = handles.incrementAndGet()
                        if (numHandles >= 100000) {
                            //scalastyle:off println
                            println(s"FileUtils - deleteRecursivelyOlderThan -> attempting gc at next 100K+ handles detected ($numHandles) ...")
                            handles.set(0)
                            Try(System.gc())
                            println(s"FileUtils - deleteRecursivelyOlderThan -> gc complete ...")
                            //scalastyle:on println
                        }
                    }

                    if (isPathModTimeGTMillis(file, ageMillis)) {
                        // file or dir that is older than age
                        tryDeleteFileOrDir(file)
                        FileVisitResult.CONTINUE
                    } else if (Files.isDirectory(file) && !Files.isSameFile(root, file)) {
                        // dir that is newer than age
                        FileVisitResult.SKIP_SUBTREE
                    } else {
                        // file that is newer than age
                        FileVisitResult.CONTINUE
                    }

                }

                override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult = {
                    if (isPathModTimeGTMillis(dir, ageMillis)) {
                        FileVisitResult.CONTINUE
                    } else if (Files.isDirectory(dir) && !Files.isSameFile(root, dir)) {
                        // dir that is newer than age
                        FileVisitResult.SKIP_SUBTREE
                    } else {
                        FileVisitResult.CONTINUE
                    }
                }

                override def postVisitDirectory(dir: Path, exception: IOException): FileVisitResult = {
                    if (
                        (!keepRoot || dir.compareTo(root) != 0) && isEmptyDir(dir)
                            && isPathModTimeGTMillis(dir, ageMillis)
                    ) {
                        tryDeleteFileOrDir(dir)
                    }
                    FileVisitResult.CONTINUE
                }
            })
        }
    }

    def deleteRecursivelyOlderThan(root: String, ageMinutes: Int, keepRoot: Boolean): Unit = {
        deleteRecursivelyOlderThan(Paths.get(root), ageMinutes, keepRoot)
    }

    def deleteRecursivelyOlderThan(root: File, ageMinutes: Int, keepRoot: Boolean): Unit = {
        deleteRecursivelyOlderThan(root.toPath, ageMinutes, keepRoot)
    }

    def isEmptyDir(dir: Path): Boolean = {
        if (Files.exists(dir) && Files.isDirectory(dir)) {
            !Files.list(dir).findAny().isPresent
        } else {
            false
        }
    }

    def isEmptyDir(dir: File): Boolean = {
        isEmptyDir(dir.toPath)
    }

    def isEmptyDir(dir: String): Boolean = {
        isEmptyDir(Paths.get(dir))
    }

    def isPathModTimeGTMillis(p: Path, ageMillis: Long): Boolean = {
        val diff = System.currentTimeMillis() - Files.getLastModifiedTime(p).toMillis
        diff > ageMillis
    }

    /** @return whether sudo is supported in this env. */
    def withSudo: Boolean = {
        val stdout = new StringBuilder()
        val stderr = new StringBuilder()
        val status = "id -u -n" ! ProcessLogger(stdout append _, stderr append _)
        val result = stdout.toString() != "root" // user needs sudo
        //scalastyle:off println
        println(s"FileUtils - does this env need sudo (non-root)? $result (out? '$stdout', err: '$stderr', status: $status)")
        //scalastyle:on println
        result
    }

    /**
     * Deletes Files associated with a tile from the provided dataframe.
     * - To avoid repeat calculations that generate new tiles,
     *   recommend caching the dataframe during execution that generates the tiles.
     * - It uses [[RASTER_PATH_KEY]].
     * - If file path provided, it deletes at the parent path level.
     * - If dir provided, that is the delete level.
     * - It only deletes if the dir starts with fuse checkpoint path
     * - It does not delete if the dir is the same as the checkpoint path,
     *   should be a sub-directory.
     *
     * @param dfIn
     *   Dataframe holding the tiles. If it hasn't been cached or isn't backed by a table,
     *   then execution may very well just generate new tiles, making this op kind of pointless.
     * @param colName
     *   column name to select in `dfIn`, default is 'tile'.
     * @param doExplode
     *   Whether to explode the tiles, means they are a collection per row, default is false.
     * @param handleCache
     *   Whether to uncache `dfIn` at the end of the deletions, default is true.
     * @param verboseLevel
     *   Get some information about the operation (0,1,2), default is 0.
     * @param msg
     *   If provided, print message to identify what is being deleted, default is "".
     * @return
     *   2-Tuple of Longs for `(deletes.length, noDeletes.length)`.
     */
    def deleteDfTilePathDirs(
                                dfIn: DataFrame,
                                colName: String = "tile",
                                doExplode: Boolean = false,
                                handleCache: Boolean = true,
                                verboseLevel: Int = 0,
                                msg: String = ""
                            ): (Long, Long) = {
        //scalastyle:off println
        if (msg.nonEmpty && verboseLevel >= 2) println(s"Deleting df tile paths -> $msg")
        try {
            var df: DataFrame = dfIn
            // explode
            if (doExplode && df != null && df.columns.contains(colName)) {
                df = df.select(
                    explode(col(colName)).alias(colName)
                )
            }

            // delete
            val paths =
                if (df == null || df.count() == 0 || !df.columns.contains(colName)) Array.empty[String]
                else {
                    df
                        .select(colName)
                        .collect()
                        .map { row =>
                            row
                                .asInstanceOf[GenericRowWithSchema]
                                .get(0)
                                .asInstanceOf[GenericRowWithSchema]
                                .getAs[Map[String, String]](2)(RASTER_PATH_KEY)
                        }
                }

            if (verboseLevel >= 2) println(s"tile paths (length) -> ${paths.length}")
            if (verboseLevel >= 2 && paths.length > 0) println(s"tile paths (first) ->  ${paths(0)}")

            val checkDir = GDAL.getCheckpointDir
            val checkPath = Paths.get(checkDir)

            val deleteStats = paths.map {
                p =>
                    val path = Paths.get(p)
                    val parentPath =
                        if (Files.isDirectory(path)) path // <- use dir if that is what was provided
                        else path.getParent               // <- otherwise, use the dir to the file
                    if (parentPath.startsWith(checkDir) && parentPath.compareTo(checkPath) != 0) {
                        // tuple of whether delete was success and the provided path
                        (Try {
                            FileUtils.deleteRecursively(parentPath, keepRoot = false)
                        }.isSuccess, p)
                    } else {
                        (false, p)
                    }
            }
            val (deletes, noDeletes) = deleteStats.partition(_._1) // true goes to deletes
            if (verboseLevel >= 2) println(s" df -> # deleted? ${deletes.length} , # not? ${noDeletes.length}")
            (deletes.length, noDeletes.length)
        } finally {
            if (handleCache) Try(dfIn.unpersist())
        }
        //scalastyle:on println
    }

}
