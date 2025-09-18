package com.dblabs.spatial.util

import org.apache.hadoop.util.hash.MurmurHash
import org.apache.spark.util.SerializableConfiguration

import java.nio.file.{Files, Path, Paths}

object NodeFilePathUtil {

    private val hasher = MurmurHash.getInstance()
    val rootPath: Path = Paths.get("/tmp/gdal_local_files")
    private val locksMap = scala.collection.mutable.Map[String, String]()

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

    private def canRead(remote: String): (Boolean, Int) = {
        // both file and locks must exist
        // they will be created by the writer node at the same time
        // if locks directory does not exist, it means the file is not being written or read
        // or we are in the process of deleting the file
        val fileExists = nodeFilePath(remote).toFile.exists()
        val locksExists = locksPath(remote).toFile.exists()
        if (fileExists && locksExists) {
            // all locks are named l1, l2, ..., ln
            // l0 is the write lock
            val allLocks = locksPath(remote).toFile.listFiles().map(_.getName).filter(_.startsWith("l"))
            val beingWritten = allLocks.exists(_.endsWith("0"))
            // check if any read lock exists
            // if maxN is 0, then no read locks exist
            val maxN = allLocks.map(_.substring(1).toInt).maxOption.getOrElse(0)
            if (maxN > 0) {
                // there are read locks present
                // the file is safe to read
                (true, maxN + 1) // +1 to account for the read lock we are about to create
            } else if (beingWritten) {
                // file is being written, we cannot read it
                // -1 indicates that the file is being written
                // wait until != -1
                (false, -1)
            } else {
                // locks directory exists, but no locks are present
                // file will be deleted soon, not safe to read
                // -2 indicates that we are in the race state
                // wait until >= 0
                (false, -2)
            }
        } else {
            (false, 0) // file does not exist, to read it we need to write it first
        }
    }

    private def updateWriteToReadLock(remotePath: String): Unit = {
        val locks = locksPath(remotePath)
        if (locks.toFile.exists()) {
            // create read lock l1 first
            // this is to ensure that we do not have
            // another writer trying to write to the same file
            val readLock = locks.resolve("l1")
            readLock.toFile.createNewFile() // create read lock
            // remove write lock l0
            val writeLock = locks.resolve("l0")
            writeLock.toFile.delete()
        }
    }

    private def writeLock(remotePath: String): Int = {
        val locks = locksPath(remotePath)
        if (!locks.toFile.exists()) {
            locks.toFile.mkdirs() // create locks directory
            val writeLock = locks.resolve("l0") // write lock is always l0
            writeLock.toFile.createNewFile() // create write lock
            1 // return 1 to indicate that write lock was created
        } else {
            // if locks directory exists, we should check if l0 exists or ln exists
            val l0 = locks.resolve("l0").toFile.exists()
            val maxLn = locks.toFile
                .listFiles()
                .filter(_.getName.startsWith("l"))
                .map(_.getName.substring(1).toInt)
                .maxOption
                .getOrElse(0)
            if (!l0 && maxLn == 0) {
                val writeLock = locks.resolve("l0") // write lock is always l0
                writeLock.toFile.createNewFile() // create write lock
                1 // return 1 to indicate that write lock was created
            } else {
                0 // return 0 to indicate that write lock was not created
            }
        }
    }

    private def deleteLocalFileWithSiblings(localPath: String): Int = {
        val path = Paths.get(localPath)
        val fileName = path.getFileName.toString.split("\\.").head
        val locks = Paths.get(s"${localPath}_locks")
        val siblings = path.getParent.toFile.listFiles().filter(f => {
            f.getName.startsWith(fileName) || f.getName.startsWith(s".$fileName")
        })
        if (siblings.nonEmpty) {
            siblings.foreach(_.delete())
            if (locks.toFile.exists()) {
                locks.toFile.delete()
            }
            if (path.getParent.toFile.listFiles().isEmpty) {
                // if parent directory is empty, delete it
                Files.deleteIfExists(path.getParent)
            }
            1 // return 1 to indicate that local file and its siblings were removed
        } else {
            0 // return 0 to indicate that no files were removed
        }
    }

    def readLock(remotePath: String, hconf: SerializableConfiguration): (String, Int) = {
        val localPath = this.nodeFilePath(remotePath).toString
        def untilCanRead(): Int = {
            var (canRead, n) = this.canRead(remotePath)
            while (!canRead && n <= 0) {
                Thread.sleep(10) // wait until the file is ready to be read
                val (c, k) = this.canRead(remotePath)
                canRead = c
                n = k
            }
            n
        }
        val locks = locksPath(remotePath)
        if (!locks.toFile.exists()) {
            // nobody read this file yet, it is not written yet
            locks.toFile.mkdirs() // create locks directory
            // get write lock l0
            val c = writeLock(remotePath)
            if (c == 0) {
                // if write lock was not created, it means that the file is being written by another node
                // we should not create read lock, we need to wait until the file is ready to be read
                val n = untilCanRead() // wait until the file is ready to be read
                val k = math.max(n, 1) // ensure that n is at least 1
                val readLock = locks.resolve(s"l$k") // read lock is always l1, l2, ..., ln
                readLock.toFile.createNewFile() // create read lock
                locksMap.addOne((remotePath, s"l$k")) // store the lock in the map
                (localPath, k) // return n to indicate that read lock was created
            } else {
                // write lock created
                HadoopUtils.copyToPath(remotePath, localPath, hconf) // copy the file to the local path
                updateWriteToReadLock(remotePath) // update the lock from write to read
                locksMap.addOne((remotePath, "l1")) // store the lock in the map
                (localPath, 1) // return 1 to indicate that read lock was created
            }
        } else {
            val n = untilCanRead() // wait until the file is ready to be read
            val k = math.max(n, 1) // ensure that n is at least 1
            val readLock = locks.resolve(s"l$k") // read lock is always l1, l2, ..., ln
            readLock.toFile.createNewFile() // create read lock
            locksMap.addOne((remotePath, s"l$k")) // store the lock in the map
            (localPath, k)
        }
    }

    def releaseReadLock(remotePath: String, hconf: SerializableConfiguration): Int = {
        val lock = locksMap.get(remotePath)
        val localPath = this.nodeFilePath(remotePath).toString
        if (lock.isDefined) {
            val lockName = lock.get
            locksMap.remove(remotePath) // remove the lock from the map
            val locks = locksPath(remotePath)
            if (locks.toFile.exists()) {
                // if locks directory exists, we can delete the read lock
                val readLock = locks.resolve(lockName)
                if (readLock.toFile.exists()) {
                    readLock.toFile.delete() // delete the read lock
                    // check if locks directory is empty
                    locks.toFile.listFiles() match {
                        case null                   =>
                            // if listFiles returns null, it means the directory is empty
//                            Files.deleteIfExists(locks)
//                            Files.deleteIfExists(Paths.get(localPath))
                            deleteLocalFileWithSiblings(localPath)
                        case files if files.isEmpty =>
                            // if there are no files in the directory
//                            Files.deleteIfExists(locks)
//                            Files.deleteIfExists(Paths.get(localPath))
                            deleteLocalFileWithSiblings(localPath)
                        case _                      => // if there are still files in the directory, do nothing
                    }
                }
            }
            1 // return 1 to indicate that read lock was released
        } else {
            0 // if lock was not found, return 0
        }
    }

}
