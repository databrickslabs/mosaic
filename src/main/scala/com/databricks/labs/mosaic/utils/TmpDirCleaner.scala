package com.databricks.labs.mosaic.utils

import com.databricks.labs.mosaic.functions.MosaicContext

import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{DirectoryStream, Files, Path, Paths}
import java.time.Instant
import java.util.Comparator
import java.util.concurrent.{Callable, ForkJoinPool, TimeUnit, TimeoutException}
import scala.collection.JavaConverters._

object TmpDirCleaner {

    private val interval = 1000L // 1s

    def collectEmptyTmpDirs(): Unit = {
        val now = System.currentTimeMillis()

        val cutoff = Instant.ofEpochMilli(now - interval)
        val root = Paths.get(MosaicContext.tmpDir(null)).getParent
        if (!Files.isDirectory(root)) return

        // Walk all sub-dirs, reverse sort so children go first
        Files
            .walk(root)
            .filter(path => Files.isDirectory(path))
            .sorted(Comparator.reverseOrder[Path]())
            .iterator()
            .asScala
            .foreach { path =>
                try {
                    val attrs = Files.readAttributes(path, classOf[BasicFileAttributes])
                    if (attrs.lastModifiedTime().toInstant.isBefore(cutoff) && isEmptyDir(path)) {
                        tryDeleteWithTimeout(path)
                    }
                } catch {
                    case _: IOException => // ignore transient failures
                }
            }
    }

    private def isEmptyDir(path: Path): Boolean = {
        var stream: DirectoryStream[Path] = null
        try {
            stream = Files.newDirectoryStream(path)
            !stream.iterator().hasNext
        } catch {
            case _: IOException => false
        } finally {
            if (stream != null) stream.close()
        }
    }

    private def tryDeleteWithTimeout(path: Path, timeoutSecs: Long = 10): Boolean = {
        val task = ForkJoinPool
            .commonPool()
            .submit(new Callable[Boolean] {
                override def call(): Boolean = {
                    Files.delete(path)
                    true
                }
            })
        try {
            task.get(timeoutSecs, TimeUnit.SECONDS)
        } catch {
            case _: TimeoutException =>
                task.cancel(true)
                false
            case _: Exception        => false
        } finally {
            if (!task.isDone) task.cancel(true)
        }
    }

}
