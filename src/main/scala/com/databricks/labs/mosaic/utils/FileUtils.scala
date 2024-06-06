package com.databricks.labs.mosaic.utils

import com.databricks.labs.mosaic.MOSAIC_RASTER_TMP_PREFIX_DEFAULT

import java.io.{BufferedInputStream, File, FileInputStream, FilenameFilter, IOException}
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, Paths, SimpleFileVisitor}
import scala.util.Try

object FileUtils {

    val MINUTE_IN_MILLIS = 60 * 1000

    def readBytes(path: String): Array[Byte] = {
        val bufferSize = 1024 * 1024 // 1MB
        val cleanPath = PathUtils.replaceDBFSTokens(path)
        val inputStream = new BufferedInputStream(new FileInputStream(cleanPath))
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

    def createMosaicTempDir(prefix: String = MOSAIC_RASTER_TMP_PREFIX_DEFAULT): String = {
        val tempRoot = Paths.get(s"$prefix/mosaic_tmp/")
        if (!Files.exists(tempRoot)) {
            Files.createDirectories(tempRoot)
        }
        val tempDir = Files.createTempDirectory(tempRoot, "mosaic")
        tempDir.toFile.getAbsolutePath
    }

    /**
     * Delete files recursively (no conditions).
     * @param root
     *   May be a directory or a file.
     * @param keepRoot
     *   If true, avoid deleting the root dir itself.
     */
    def deleteRecursively(root: Path, keepRoot: Boolean): Unit = {

        Files.walkFileTree(root, new SimpleFileVisitor[Path] {
            override def visitFile(file: Path, attributes: BasicFileAttributes): FileVisitResult = {
                Try(Files.delete(file))
                FileVisitResult.CONTINUE
            }

            override def postVisitDirectory(dir: Path, exception: IOException): FileVisitResult = {
                if ((!keepRoot || dir.compareTo(root) != 0) && isEmptyDir(dir)) {
                    Try(Files.delete(dir))
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
                        Try(Files.delete(file))
                    }
                    FileVisitResult.CONTINUE
                }

                override def postVisitDirectory(dir: Path, exception: IOException): FileVisitResult = {
                    if (
                        (!keepRoot || dir.compareTo(root) != 0) && isEmptyDir(dir)
                            && isPathModTimeGTMillis(dir, ageMillis)
                    ) {
                        Try(Files.delete(dir))
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

}
