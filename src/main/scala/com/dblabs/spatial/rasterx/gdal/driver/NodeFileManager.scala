package com.dblabs.spatial.rasterx.gdal.driver

import org.apache.spark.util.SerializableConfiguration

import scala.collection.mutable

object NodeFileManager {

    // remoteFilePath -> localFilePath
    private val localFiles = mutable.Map[String, (String, Int)]()
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
        else localFiles.update(remotePath, (localPath, 1))
    }

    def releaseRemote(remotePath: String): Unit = {
        val n = removeJVMReadLock(remotePath)
        if (n == 0) {
            // If no JVM read locks left, release the read lock on the node
            NodeFilePathUtil.releaseReadLock(remotePath, hconf)
        }
    }

    private def removeJVMReadLock(remotePath: String): Int = {
        localFiles.get(remotePath) match {
            case Some((localPath, n)) =>
                if (n > 1) {
                    localFiles.update(remotePath, (localPath, n - 1))
                    n - 1
                } else {
                    localFiles.remove(remotePath)
                    0
                }
            case None                 => 0
        }
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
