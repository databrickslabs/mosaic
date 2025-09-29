package com.databricks.labs.gbx.util

import org.apache.hadoop.util.hash.MurmurHash
import org.apache.spark.util.SerializableConfiguration

import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap}
import scala.util.control.NonFatal

object NodeFilePathUtil {

    private val hasher = MurmurHash.getInstance()
    private val uuid = this.hashCode().toHexString
    val rootPath: Path = Paths.get(s"/tmp/gdal_local_files/$uuid")
    private val maxRetries = 3
    private val tinyBackoffMs = 2L

    private final case class Entry(dir: Path, ready: CompletableFuture[Path], readers: AtomicInteger)
    private val entries = new ConcurrentHashMap[String, Entry]()

    private def filename(remote: String): String = {
        val noPrefix = remote.split("://").last
        val i = noPrefix.lastIndexOf("/")
        if (i >= 0) noPrefix.substring(i + 1)
        else noPrefix
    }

    private def murmur(s: String): String = s"mm3_${hasher.hash(s.getBytes).toString.replace("-", "_")}"
    private def base(remote: String): String = remote.split("://").last.split('/').last
    private def cacheDir(remote: String): Path = rootPath.resolve(murmur(remote))
    private def primary(remote: String): Path = cacheDir(remote).resolve(base(remote))
    private def murmurParent(remote: String): String = s"mm3_${hasher.hash(remote.getBytes).toString.replace("-", "_")}"
    private def parentDir(remote: String): Path = rootPath.resolve(murmurParent(remote))
    private def nodeFilePath(remote: String): Path = parentDir(remote).resolve(filename(remote))

    /**
      * Ensure copy exists (primary + sidecars via HadoopUtils) and acquire read
      * lock.
      */
    def readLock(remotePath: String, hconf: SerializableConfiguration): (String, Int) = {
        val localPath = nodeFilePath(remotePath)
        val key = localPath.toString
        var attempts = 0
        var lastErr: Throwable = null

        while (attempts <= maxRetries) {
            attempts += 1
            var e = entries.get(key)
            if (e == null) {
                val created = Entry(localPath, new CompletableFuture[Path](), new AtomicInteger(0))
                val prev = entries.putIfAbsent(key, created)
                if (prev == null) {
                    // I am the copier
                    try {
                        Files.createDirectories(localPath.getParent)
                        HadoopUtils.copyToPath(remotePath, localPath.toString, hconf) // copies primary + sidecars
                        created.ready.complete(localPath)
                        e = created
                    } catch {
                        case NonFatal(err) =>
                            created.ready.completeExceptionally(err)
                            lastErr = err
                            scala.util.Try(deleteWithSiblings(localPath.toString))
                            entries.remove(key, created)
                            Thread.`yield`(); if (tinyBackoffMs > 0) Thread.sleep(tinyBackoffMs)
                            e = null // retry loop; a waiter may become the next copier
                    }
                } else {
                    e = prev
                }
            }

            if (e != null) {
                try {
                    e.ready.join() // throws if copier failed
                    val n = e.readers.incrementAndGet()
                    return (primary(remotePath).toString, n)
                } catch {
                    case ex: Throwable =>
                        lastErr = Option(ex.getCause).getOrElse(ex)
                        entries.remove(key, e) // allow a fresh attempt
                        Thread.`yield`(); if (tinyBackoffMs > 0) Thread.sleep(tinyBackoffMs)
                    // loop to retry
                }
            }
        }
        throw new RuntimeException(s"Failed to materialize $remotePath after $maxRetries retries.", lastErr)
    }

    /** Release read lock; delete cache dir when refcount hits zero. */
    def releaseReadLock(remotePath: String, hconf: SerializableConfiguration): Int = {
        val localPath = nodeFilePath(remotePath)
        val key = localPath.toString
        val e = entries.get(key)
        if (e == null) return 0
        val n = e.readers.decrementAndGet()
        if (n <= 0) {
            scala.util.Try(deleteWithSiblings(localPath.toString))
            entries.remove(key, e)
            0
        } else n
    }

    private def deleteWithSiblings(localPath: String): Unit = {
        val path = Paths.get(localPath)
        val fileName = path.getFileName.toString.split("\\.").head
        val parent = path.getParent
        val siblings = Option(parent.toFile.listFiles())
            .getOrElse(Array.empty)
            .filter(f => {
                f.getName.startsWith(fileName) || f.getName.startsWith(s".$fileName")
            })
        if (siblings.nonEmpty) siblings.foreach(s => Files.deleteIfExists(s.toPath))
        val remaining = Option(parent.toFile.listFiles()).getOrElse(Array.empty)
        if (remaining.isEmpty) Files.deleteIfExists(parent)
    }

}
