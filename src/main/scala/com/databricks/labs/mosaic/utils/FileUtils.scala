package com.databricks.labs.mosaic.utils

import java.io.{BufferedInputStream, File, FileInputStream, IOException}
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file._
import java.util.concurrent.atomic.AtomicInteger
import scala.util.Try

object FileUtils {

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

    def createMosaicTempDir(prefix: String = "/tmp"): String = {
        val tempRoot = Paths.get(s"$prefix/mosaic_tmp/")
        if (!Files.exists(tempRoot)) {
            Files.createDirectories(tempRoot)
        }
        val tempDir = Files.createTempDirectory(tempRoot, "mosaic")
        tempDir.toFile.getAbsolutePath
    }

    /** Delete provided path (only deletes empty dirs). */
    def tryDeleteFileOrDir(path: Path): Boolean = {
            Try(Files.delete(path)).isSuccess
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

    def isEmptyDir(dir: Path): Boolean = {
        if (Files.exists(dir) && Files.isDirectory(dir)) {
            !Files.list(dir).findAny().isPresent
        } else {
            false
        }
    }

}
