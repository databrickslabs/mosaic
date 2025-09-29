package com.databricks.labs.gbx.util

import com.google.common.io.{ByteStreams, Closeables}
import org.apache.hadoop.fs._
import org.apache.orc.util.Murmur3
import org.apache.spark.util.SerializableConfiguration

import java.net.URI
import scala.collection.mutable

//noinspection ScalaWeakerAccess
object HadoopUtils {

    var hadoopConf: SerializableConfiguration = _

    def setHadoopConf(hconf: SerializableConfiguration): Unit = {
        hadoopConf = hconf
    }

    def cleanPath(inPath: String): String = {
        inPath match {
            // Handle Unity Catalog Volumes path
            case _ if inPath.startsWith("/Volumes/")     => inPath
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

    def getFirstFile(inPath: String, hconf: SerializableConfiguration): String = {
        val path = new Path(new URI(cleanPath(inPath)))
        val fs = path.getFileSystem(hconf.value)
        fs.listFiles(path, false).next().getPath.toString
    }

    def listHadoopDirs(inPath: String, hconf: SerializableConfiguration): Seq[String] = {
        val path = new Path(new URI(cleanPath(inPath)))
        val fs = path.getFileSystem(hconf.value)
        if (!fs.exists(path)) Seq.empty[String]
        else fs
            .listStatus(path)
            .filter(_.isDirectory)
            .map(_.getPath.toString)
    }

    def listAllHadoopFiles(
        inPath: String,
        hconf: SerializableConfiguration,
        regexFilter: String,
        dropEmpty: Boolean = false
    ): mutable.Seq[String] = {
        val filter = if (regexFilter == "") ".*" else s".*$regexFilter.*"
        val path = new Path(new URI(cleanPath(inPath)))
        val fs = path.getFileSystem(hconf.value)
        val it = fs.listFiles(path, true) // recursive
        val files = scala.collection.mutable.ArrayBuffer[String]()
        while (it.hasNext) {
            val fileStatus = it.next()
            if (!dropEmpty || fileStatus.getLen > 0) {
                if (regexFilter == "" || filter == ".*") {
                    files += fileStatus.getPath.toString
                } else if (fileStatus.getPath.toString.matches(filter)) {
                    files += fileStatus.getPath.toString
                }
            }
        }
        files
    }

    def copyToPath(
        inPath: String,
        outPath: String,
        hconf: SerializableConfiguration
    ): String = {
        val copyFromPath = new Path(cleanPath(inPath))
        val srcFS = copyFromPath.getFileSystem(hconf.value)
        val srcStatus = srcFS.getFileStatus(copyFromPath)
        val outputDir =
            if (srcStatus.isDirectory) {
                new Path(cleanPath(outPath)).toString
            } else {
                new Path(cleanPath(outPath)).getParent.toString
            }
        copyToLocalDir(copyFromPath.toString, outputDir, hconf)
    }

    def copyRelativeFiles(
        srcFs: FileSystem,
        dstFs: FileSystem,
        baseSrcPath: Path,
        dstDirPath: Path
    ): Unit = {
        val extension = baseSrcPath.getName.split("\\.").lastOption.getOrElse("")
        val baseName = baseSrcPath.getName.stripSuffix(s".$extension")
        val prefix = baseName + "."

        val filter = new PathFilter {
            override def accept(path: Path): Boolean = path.getName.startsWith(prefix)
        }

        val parentDir = baseSrcPath.getParent
        val matchingFiles = srcFs.listStatus(parentDir, filter)

        matchingFiles.foreach { fileStatus =>
            val srcFile = fileStatus.getPath
            val dstFile = new Path(dstDirPath, srcFile.getName)
            AtomicDistributedCopy.copyIfNeeded(srcFs, dstFs, srcFile, dstFile)
        }
    }

    def copyToLocalDir(inPath: String, outDir: String, hConf: SerializableConfiguration): String = {
        val copyFromPath = new Path(cleanPath(inPath))
        val outDirPath = new Path(cleanPath(outDir))
        val srcFS = copyFromPath.getFileSystem(hConf.value)
        val dstFS = outDirPath.getFileSystem(hConf.value)

        if (!dstFS.exists(outDirPath)) dstFS.mkdirs(outDirPath)

        if (srcFS.getFileStatus(copyFromPath).isDirectory) {
            val dst = new Path(outDirPath, copyFromPath.getName)
            AtomicDistributedCopy.copyIfNeeded(srcFS, dstFS, copyFromPath, dst)
            dst.toString
        } else {
            if (!dstFS.exists(outDirPath)) dstFS.mkdirs(outDirPath)
            copyRelativeFiles(srcFS, dstFS, copyFromPath, outDirPath)
            val fileName = copyFromPath.getName
            s"$outDirPath/$fileName"
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
