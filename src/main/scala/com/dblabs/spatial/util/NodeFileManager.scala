package com.dblabs.spatial.util

import org.apache.spark.util.SerializableConfiguration

import scala.collection.mutable

object NodeFileManager {

    // remoteFilePath -> localFilePath
    private val localFiles = mutable.Map[String, (String, Int)]() // remoteFilePath -> (localFilePath, readLocksCount)
    private val local2Remote = mutable.Map[String, String]() // localFilePath -> remoteFilePath
    private var hconf: SerializableConfiguration = _

    def init(hadoopConf: SerializableConfiguration): Unit = {
        hconf = hadoopConf
    }

    def readRemote(remotePath: String): String = {
        val (localPath, _) = NodeFilePathUtil.readLock(remotePath, hconf) // Create a read lock and make sure file exists
        addJVMReadLock(remotePath, localPath) // Ensure that JVM has read locks count updated
        localPath
    }

    private def addJVMReadLock(remotePath: String, localPath: String): Unit = {
        val n = localFiles.getOrElse(remotePath, (localPath, 0))._2
        if (n > 0) localFiles.update(remotePath, (localPath, n + 1))
        else {
            localFiles.update(remotePath, (localPath, 1))
            local2Remote.update(localPath, remotePath) // Keep track of the mapping from local to remote
        }
    }

    def releaseRemote(remotePath: String): Unit = {
        // Get the remote path from local2Remote map, if it exists
        // This has to happen before we remove JVM read lock
        // as this release could remove the lookup from local2Remote map
        val remote = local2Remote.getOrElse(remotePath, "")
        val n = removeJVMReadLock(remotePath)
        if (n == 0) {
            // If no JVM read locks left, release the read lock on the node
            val try1 = NodeFilePathUtil.releaseReadLock(remotePath, hconf)
            if (try1 == 0) {
                NodeFilePathUtil.releaseReadLock(remote, hconf)
            }
        }
    }

    private def removeJVMReadLock(remotePath: String): Int = {
        def releaseRemote(remote: String): Int = {
            localFiles.get(remote) match {
                case Some((localPath, n)) =>
                    if (n > 1) {
                        localFiles.update(remote, (localPath, n - 1))
                        n - 1
                    } else {
                        localFiles.remove(remote)
                        local2Remote.remove(localPath)
                        0
                    }
                case None                 => 0
            }
        }
        val try1 = releaseRemote(remotePath)
        if (try1 > 0) return try1
        // If the remote path was not found, check if it was passed as a local path
        // Note: it is perfectly fine to getOrElse "" here, as it means the file was not found in the localFiles map
        // and releasing would result in no-op
        val remote = local2Remote.getOrElse(remotePath, "")
        releaseRemote(remote)
    }

    def getHConf: SerializableConfiguration = {
        if (hconf == null) {
            throw new IllegalStateException("NodeFileManager is not initialized. Call init() first.")
        }
        hconf
    }

    def getLocalPath(remotePath: String): Option[String] = {
        localFiles.get(remotePath) match {
            case Some((localPath, _)) => Some(localPath)
            case None                 => None
        }
    }

    def removeFile(remotePath: String): Unit = {
        localFiles.remove(remotePath)
    }

}
