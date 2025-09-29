package com.databricks.labs.gbx.util

import org.apache.spark.util.SerializableConfiguration

import scala.collection.mutable

object NodeFileManager {

    // remoteFilePath -> localFilePath
    private val local2Remote = mutable.Map[String, String]() // localFilePath -> remoteFilePath
    private var hconf: SerializableConfiguration = _

    def init(hadoopConf: SerializableConfiguration): Unit = {
        hconf = hadoopConf
    }

    def readRemote(remotePath: String): String = {
        val (localPath, _) = NodeFilePathUtil.readLock(remotePath, hconf) // Create a read lock and make sure file exists
        // addJVMReadLock(remotePath, localPath) // Ensure that JVM has read locks count updated
        local2Remote.update(localPath, remotePath)
        localPath
    }

    def releaseRemote(remotePath: String): Unit = {
        val remote = local2Remote.getOrElse(remotePath, "")
        NodeFilePathUtil.releaseReadLock(remote, hconf)
    }

}
