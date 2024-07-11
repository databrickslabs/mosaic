package com.databricks.labs.mosaic.utils

import com.databricks.labs.mosaic.MOSAIC_RASTER_TMP_PREFIX_DEFAULT
import com.databricks.labs.mosaic.core.raster.io.CleanUpManager

import java.io.{BufferedInputStream, File, FileInputStream, IOException}
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, Paths, SimpleFileVisitor}
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
     * Delete files recursively (no conditions).
     *
     * @param root
     *   May be a directory or a file.
     * @param keepRoot
     *   If true, avoid deleting the root dir itself.
     */
    def deleteRecursively(root: Path, keepRoot: Boolean): Unit = {

        Files.walkFileTree(root, new SimpleFileVisitor[Path] {
            override def visitFile(file: Path, attributes: BasicFileAttributes): FileVisitResult = {
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
                override def visitFile(file: Path, attributes: BasicFileAttributes): FileVisitResult = {

                    if (isPathModTimeGTMillis(file, ageMillis)) {
                        // file or dir that is older than age
                        tryDeleteFileOrDir(file)
                        FileVisitResult.CONTINUE
                    } else if (Files.isDirectory(file) && !Files.isSameFile(root, file)) {
                        //scalastyle:off println
                        //println(s"DELETE -> skipping subtree under dir '${file.toString}'")
                        //scalastyle:on println

                        // dir that is newer than age
                        FileVisitResult.SKIP_SUBTREE
                    } else {
                        // file that is newer than age
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

}
