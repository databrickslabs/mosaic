package com.databricks.labs.mosaic.utils

import com.databricks.labs.mosaic.functions.MosaicContext
import com.google.common.io.{ByteStreams, Closeables}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.orc.util.Murmur3
import org.apache.spark.sql.execution.streaming.FileSystemBasedCheckpointFileManager
import org.apache.spark.util.SerializableConfiguration

import java.net.URI

//noinspection ScalaWeakerAccess
object HadoopUtils {

    var hadoopConf: SerializableConfiguration = _

    def setHadoopConf(hconf: SerializableConfiguration): Unit = {
        hadoopConf = hconf
    }

    def cleanPath(inPath: String): String = {
        inPath match {
            // Handle Unity Catalog Volumes path
            case _ if inPath.startsWith("/dbfs/Volume/") => inPath.replace("/dbfs/Volume/", "/Volume/")
            // If it isn't a volumes path but starts with /dbfs/ then it is a DBFS path
            // Hadoop will not work with this path so we need to replace it with dbfs:/
            case _ if inPath.startsWith("/dbfs/")        => inPath.replace("/dbfs/", "dbfs:/")
            // If it is a local path, we need to replace the /tmp/ with file:/tmp/
            // This is because Hadoop will interpret any path as /dbfs/ location unless it is prefixed with file:/
            case _ if inPath.startsWith("/tmp/")         => inPath.replace("/tmp/", "file:/tmp/")
            // If the path is starting with file:/ keep it that way, it is the local file system
            case _ if inPath.startsWith("file:/")        => inPath
            // If the path is starting with dbfs:/ keep it that way, it is the DBFS file system
            case _ if inPath.startsWith("dbfs:/")        => inPath
            // All other paths are considered as local paths
            case _ if inPath.startsWith("/")             => s"file:$inPath"
            case _                                       => s"file:/$inPath"
        }
    }

    def getRelativePath(inPath: String, basePath: String): String = {
        inPath
            .stripPrefix(basePath)
            .stripPrefix("file:/")
            .stripPrefix("dbfs:/")
            .stripPrefix("/dbfs/")
            .stripPrefix("dbfs/")
            .stripPrefix("Volumes/")
            .stripPrefix("/Volumes/")
    }

    def listHadoopFiles(inPath: String): Seq[String] = {
        listHadoopFiles(inPath, hadoopConf)
    }

    def listHadoopFiles(inPath: String, hconf: SerializableConfiguration): Seq[String] = {
        val path = new Path(new URI(cleanPath(inPath)))
        val fs = path.getFileSystem(hconf.value)
        fs.listStatus(path)
            .filterNot(_.isDirectory)
            .map(_.getPath.toString)
    }

    def copyToLocalTmp(inPath: String, hconf: SerializableConfiguration): String = {
        val copyFromPath = new Path(cleanPath(inPath))
        val outputDir = cleanPath(MosaicContext.tmpDir(null))
        copyToLocalDir(copyFromPath.toString, outputDir, hconf)
    }

    def copyToLocalDir(inPath: String, outDir: String, hConf: SerializableConfiguration, basePath: String = ""): String = {
        val copyFromPath = new Path(cleanPath(inPath))
        val fs = copyFromPath.getFileSystem(hConf.value)
        val checkpointManager = new FileSystemBasedCheckpointFileManager(new Path(outDir), hConf.value)
        checkpointManager.createCheckpointDirectory()

        if (fs.getFileStatus(copyFromPath).isDirectory) {
            val files = listHadoopFiles(copyFromPath.toString, hConf)
            files.foreach(filePath => copyToLocalDir(filePath, outDir, hConf, basePath = copyFromPath.toString))
            outDir
        } else {
            val relativePath = new Path(getRelativePath(copyFromPath.toString, basePath))
            val fileName = relativePath.getName
            val baseName = if (fileName.contains(".")) fileName.substring(0, fileName.lastIndexOf('.')) else fileName
            val localDestPath = new Path(s"$outDir/$relativePath")
            // this is horribly inefficient but ok for now
            // we need a set of files to check for that is fixed per format
            val parent = relativePath.getParent
            val pattern = if (parent.toString.endsWith("/")) s"$parent$baseName" else s"$parent/$baseName"
            val sideFiles = listHadoopFiles(copyFromPath.getParent.toString, hConf)
                .filter(_.contains(pattern))
            sideFiles.foreach( // copy together with sidecar files
              filePath => {
                  val input = new Path(filePath)
                  val output = new Path(localDestPath.getParent.toString + "/" + input.getName)
                  AtomicDistributedCopy.copyIfNeeded(checkpointManager, fs, input, output)
              }
            )
            localDestPath.toString
        }
    }

    /**
      * Reads the content of the file.
      * @param fs
      *   File system.
      * @param status
      *   File status.
      * @return
      *   An array of bytes.
      */
    def readContent(fs: FileSystem, status: FileStatus): Array[Byte] = {
        val stream = fs.open(status.getPath)
        try { // noinspection UnstableApiUsage
            ByteStreams.toByteArray(stream)
        } finally { // noinspection UnstableApiUsage
            Closeables.close(stream, true)
        }
    }

    def deleteIfExists(tmpPath: String, hconf: SerializableConfiguration): Unit = {
        val cleanPath = HadoopUtils.cleanPath(tmpPath)
        val path = new Path(cleanPath)
        val fs = path.getFileSystem(hconf.value)
        if (fs.exists(path)) {
            fs.delete(path, true)
        }
    }

    def getSize(path: String, hConf: SerializableConfiguration): Long = {
        val cleanPath = new Path(HadoopUtils.cleanPath(path))
        val fs = cleanPath.getFileSystem(hConf.value)
        val status = fs.getFileStatus(cleanPath)
        if (status.isDirectory) {
            fs.getContentSummary(cleanPath).getLength
        } else {
            status.getLen
        }
    }

    /**
      * Generates a UUID for the file.
      *
      * @param status
      *   File status.
      * @return
      *   A UUID.
      */
    def getUUID(status: FileStatus): Long = {
        val uuid = Murmur3.hash64(
          status.getPath.toString.getBytes("UTF-8") ++
              status.getLen.toString.getBytes("UTF-8") ++
              status.getModificationTime.toString.getBytes("UTF-8")
        )
        uuid
    }

}
