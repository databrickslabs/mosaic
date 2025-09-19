package com.dblabs.spatial.util

import org.apache.hadoop.util.hash.MurmurHash
import org.apache.spark.util.SerializableConfiguration

import java.nio.file.{FileAlreadyExistsException, Files, Path, Paths}
import java.util.concurrent.ConcurrentHashMap
import scala.util.control.NonFatal

object NodeFilePathUtil {

    private val hasher = MurmurHash.getInstance()
    val rootPath: Path = Paths.get("/tmp/gdal_local_files")
    private val locksMap = new ConcurrentHashMap[String, String]()
    private val writerTTL = 500 // milliseconds
    private val sleepTime = 10 // milliseconds
    private val maxWaitTime = 10000 // milliseconds
    private val L0 = "l0" // write lock
    private val READ_L_PATTERN = "l_\\d+_\\d+" // l_<threadId>_<nowMs>

    private def nowMs(): Long = System.currentTimeMillis()

    private def murmurParent(remote: String): String = {
        s"mm3_${hasher.hash(remote.getBytes).toString.replace("-", "_")}"
    }

    private def filename(remote: String): String = {
        val noPrefix = remote.split("://").last
        val i = noPrefix.lastIndexOf("/")
        if (i >= 0) noPrefix.substring(i + 1)
        else noPrefix
    }

    private def parentDir(remote: String): Path = rootPath.resolve(murmurParent(remote))

    private def nodeFilePath(remote: String): Path = parentDir(remote).resolve(filename(remote))

    private def locksPath(remote: String): Path = Paths.get(s"${nodeFilePath(remote)}_locks")

    private def safeList(dir: Path): Array[String] = {
        val f = dir.toFile
        if (!f.exists()) Array.empty[String]
        else {
            val files = f.listFiles()
            if (files == null) Array.empty[String]
            else files
                .map(_.getName)
                .filter(name => name == L0 || name.matches(READ_L_PATTERN))
        }
    }

    private def readerLocks(dir: Path): Array[String] = safeList(dir).filter(_ != L0)

    private def getMTime(path: Path): Long = {
        try Files.getLastModifiedTime(path).toMillis
        catch { case NonFatal(_) => 0L }
    }

    private def tryDelete(path: Path): Boolean = {
        try Files.deleteIfExists(path)
        catch { case NonFatal(_) => false }
    }

    private def isWriterStale(filePath: Path, locks: Path): Boolean = {
        val l0 = locks.resolve(L0)
        locks.toFile.exists() &&
        l0.toFile.exists() &&
        !filePath.toFile.exists() &&
        (nowMs() - getMTime(l0) > writerTTL)
    }

    private def generateReadLockName(): String = s"l_${Thread.currentThread().getId}_${nowMs()}"

    private def acquireReadLock(locks: Path): String = {
        val name = generateReadLockName()
        val p = locks.resolve(name)
        try {
            Files.createFile(p)
            name
        } catch {
            case _: FileAlreadyExistsException => name // same thread same ms; treat as already-held
        }
    }

    private def canRead(remote: String): (Boolean, Int) = {
        // both file and locks must exist
        // they will be created by the writer node at the same time
        // if locks directory does not exist, it means the file is not being written or read
        // or we are in the process of deleting the file
        val file = nodeFilePath(remote)
        val locks = locksPath(remote)
        val fileExists = file.toFile.exists()
        val locksExists = locks.toFile.exists()
        val allLocks = safeList(locks) // l\d+ only
        val hasReaders = allLocks.exists(_ != L0)
        val l0 = locks.resolve(L0)

        if (isWriterStale(file, locks)) {
            if (!fileExists && !hasReaders) {
                // stale writer, no file, no readers => reset writer lock only
                tryDelete(l0)
                // optional: if locks is now empty, remove dir
                if (allLocks.isEmpty) tryDelete(locks)
                // signal: no file, a new writer should start
                return (false, 0)
            } else if (fileExists) {
                // file published (or recovered) => just clear stale l0; never delete the file
                tryDelete(l0)
                // proceed to normal evaluation below (readers may form)
            } // if readers exist, do nothing here—let them finish
        }

        if (!locksExists) return (false, 0) // no locks dir => no file => need to write first

        if (hasReaders) {
            val nReaders = readerLocks(locks).length
            return (true, math.max(1, nReaders + 1)) // safe to read, +1 for
        }

        if (fileExists && !allLocks.contains(L0)) return (false, 1) // file exists, no readers, no writer => safe to read, create l1
        if (allLocks.contains(L0)) return (false, -1) // writer active, cannot read
        (false, 0) // no file, no readers, no writer => need to write first
    }

    private def createFile(path: Path): Int = {
        try {
            Files.createFile(path) // create file
            1
        } catch {
            case _: FileAlreadyExistsException => 0 // if file already exists, return 0
            case NonFatal(_)                   => 0 // for any other error, return 0
        }
    }

    private def updateWriteToReadLock(remotePath: String): String = {
        val locks = locksPath(remotePath)
        if (!locks.toFile.exists()) return ""
        val name = acquireReadLock(locks) // create our reader first
        val writerLock = locks.resolve(L0)
        tryDelete(writerLock) // best-effort delete write lock
        name
    }

    private def writeLock(remotePath: String): Int = {
        val locks = locksPath(remotePath)
        if (!locks.toFile.exists()) {
            locks.toFile.mkdirs()
            return createFile(locks.resolve(L0))
        }
        val l0 = locks.resolve(L0)
        val l0Exists = l0.toFile.exists()
        val readersExist = readerLocks(locks).nonEmpty
        val isStale = isWriterStale(nodeFilePath(remotePath), locks)
        if ((!l0Exists && !readersExist) || isStale) {
            if (isStale) tryDelete(l0) // best-effort delete stale l0
            createFile(l0) // create write lock or return 0 if it errors
        } else {
            0 // return 0 to indicate that write lock was not created
        }
    }

    private def deleteLocalFileWithSiblings(localPath: String): Int = {
        val path = Paths.get(localPath)
        val fileName = path.getFileName.toString.split("\\.").head
        val locks = Paths.get(s"${localPath}_locks")
        val parent = path.getParent
        val siblings = Option(parent.toFile.listFiles())
            .getOrElse(Array.empty)
            .filter(f => {
                f.getName.startsWith(fileName) || f.getName.startsWith(s".$fileName")
            })
        if (siblings.nonEmpty) {
            siblings.foreach(s => tryDelete(s.toPath))
            if (locks.toFile.exists()) tryDelete(locks) // delete locks directory if exists
            val remaining = Option(parent.toFile.listFiles()).getOrElse(Array.empty)
            if (remaining.isEmpty) {
                // if parent directory is empty, delete it
                Files.deleteIfExists(parent)
            }
            1 // return 1 to indicate that local file and its siblings were removed
        } else {
            0 // return 0 to indicate that no files were removed
        }
    }

    def readLock(remotePath: String, hconf: SerializableConfiguration): (String, Int) = {
        val localPath = this.nodeFilePath(remotePath).toString

        def untilCanRead(): Int = {
            val start = nowMs()
            var n = 0
            var can = false
            while ({
                val res = this.canRead(remotePath)
                can = res._1; n = res._2
                !can && (n <= 0) && (nowMs() - start) < maxWaitTime
            }) {
                Thread.sleep(sleepTime + (math.random * 0.1 * sleepTime).toInt)
            }
            if (!can && n <= 0) {
                // timeout
                // force write assume writer is dead and what was written is lost
                val locks = locksPath(remotePath)
                if (locks.toFile.exists() && readerLocks(locks).nonEmpty) return 1 // readers appeared; join instead of purging
                val file = nodeFilePath(remotePath)

                Option(locks.toFile.listFiles()).getOrElse(Array.empty).foreach(f => Files.deleteIfExists(f.toPath))
                Files.deleteIfExists(locks)
                Files.deleteIfExists(file)
                locks.toFile.mkdirs()
                Files.createFile(locks.resolve(L0))
                HadoopUtils.copyToPath(remotePath, localPath, hconf)
                val myReader = updateWriteToReadLock(remotePath) // capture our read lock name
                if (myReader.nonEmpty) locksMap.put(remotePath, myReader) // store the lock in the map
                return 1
            }
            n
        }

        val locks = locksPath(remotePath)
        if (!locks.toFile.exists()) locks.toFile.mkdirs()

        val _ = untilCanRead()
        val name = acquireReadLock(locks)
        locksMap.put(remotePath, name) // store the lock in the map
        val readers = readerLocks(locks).length // with our lock
        (localPath, readers)
    }

    def releaseReadLock(remotePath: String, hconf: SerializableConfiguration): Int = {
        val lock = Option(locksMap.get(remotePath))
        val localPath = this.nodeFilePath(remotePath).toString
        lock match {
            case Some(lockName) =>
                locksMap.remove(remotePath)
                val locks = locksPath(remotePath)
                if (locks.toFile.exists()) {
                    val readLock = locks.resolve(lockName)
                    if (readLock.toFile.exists()) tryDelete(readLock)
                    if (readerLocks(locks).isEmpty) {
                        tryDelete(locks)
                        deleteLocalFileWithSiblings(localPath)
                    }
                }
                1
            case None           => 0
        }
    }

}
