package com.databricks.labs.mosaic.utils

import com.databricks.labs.mosaic.functions.MosaicContext
import com.google.common.io.{ByteStreams, Closeables}
import org.apache.hadoop.fs.{FileStatus, FileSystem, FileUtil, Path}
import org.apache.orc.util.Murmur3
import org.apache.spark.util.SerializableConfiguration

import java.net.URI
import java.nio.file.{Files, Paths}
import java.util.UUID

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

    def getStemRegex(str: String): String = {
        val cleanPath = HadoopUtils.cleanPath(str)
        val fileName = new Path(cleanPath).getName
        val stemName = fileName.substring(0, fileName.lastIndexOf('.'))
        val stemEscaped = stemName.replace(".", "\\.")
        val stemRegex = s"$stemEscaped\\..*".r
        stemRegex.toString
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

    def copyToLocalTmp(inPath: String): String = {
        copyToLocalTmp(inPath, hadoopConf)
    }

    def copyToLocalTmp(inPath: String, hconf: SerializableConfiguration): String = {
        val copyFromPath = new Path(cleanPath(inPath))
        val fs = copyFromPath.getFileSystem(hconf.value)
        val uuid = UUID.randomUUID().toString.replace("-", "_")
        val outDir = MosaicContext.tmpDir(null) + s"/$uuid"
        Files.createDirectories(Paths.get(outDir))
        if (fs.getFileStatus(copyFromPath).isDirectory) {
            // If the path is a directory, we need to copy all files in the directory
            val name = copyFromPath.getName
            val stemRegex = ".*"
            wildcardCopy(copyFromPath.toString, outDir + "/" + name, stemRegex, hconf)
        } else {
            val inPathDir = copyFromPath.getParent.toString
            val stemRegex = getStemRegex(inPath)
            wildcardCopy(inPathDir, outDir, stemRegex, hconf)
        }
        val fullFileName = copyFromPath.getName.split("/").last
        // Wrapper to force metadata to be copied
        try {
            fs.getFileStatus(new Path(s"${MosaicContext.tmpDir(null)}/$uuid/$fullFileName")).getPath.toString
        } catch {
            case _: Exception =>
                // If the file is not found, we need to copy it again
                val newPath = new Path(s"${MosaicContext.tmpDir(null)}/$uuid/$fullFileName")
                fs.copyToLocalFile(copyFromPath, newPath)
                // Return the path of the copied file
        }
        fs.getFileStatus(new Path(s"${MosaicContext.tmpDir(null)}/$uuid/$fullFileName")).getPath.toString
    }

    def wildcardCopy(inDirPath: String, outDirPath: String, pattern: String): Unit = {
        wildcardCopy(inDirPath, outDirPath, pattern, hadoopConf)
    }

    def wildcardCopy(inDirPath: String, outDirPath: String, pattern: String, hconf: SerializableConfiguration): Unit = {
        val copyFromPath = cleanPath(inDirPath)
        val copyToPath = cleanPath(outDirPath)

        val tc = listHadoopFiles(copyFromPath, hconf)
            .filter(f => s"$copyFromPath/$pattern".r.findFirstIn(f).isDefined)

        for (path <- tc) {
            val src = new Path(path)
            val dest = new Path(copyToPath, src.getName)
            if (src != dest) {
                val fs = src.getFileSystem(hconf.value)
                if (fs.getFileStatus(src).isDirectory) {
                    //writeNioDir(src, dest, hconf)
                    Files.createDirectories(Paths.get(dest.toString))
                    FileUtil.copy(fs, src, fs, dest, false, hconf.value)
                } else {
                    //writeNioFile(src, dest, hconf)
                    Files.createDirectories(Paths.get(dest.getParent.toString))
                    Files.createFile(Paths.get(dest.toString))
                    fs.copyToLocalFile(src, dest)
                }
            }
        }
    }

    def writeNioFile(src: Path, dest: Path, hconf: SerializableConfiguration): Unit = {
        val fs = src.getFileSystem(hconf.value)
        val srcStatus = fs.getFileStatus(src)
        val bytes = readContent(fs, srcStatus)
        FileUtils.writeBytes(dest.toString, bytes)
    }

    def writeNioDir(src: Path, dest: Path, hconf: SerializableConfiguration): Unit = {
        val fs = src.getFileSystem(hconf.value)
        val destNio = Paths.get(dest.toString)

        def recurse(currentSrc: Path, currentDest: java.nio.file.Path): Unit = {
            fs.listStatus(currentSrc).foreach { entry =>
                val name = entry.getPath.getName
                val nextSrc = entry.getPath
                val nextDest = currentDest.resolve(name)

                if (entry.isDirectory) {
                    Files.createDirectories(nextDest)
                    recurse(nextSrc, nextDest)
                } else {
                    val destH = new Path(nextDest.toString)
                    writeNioFile(nextSrc, destH, hconf)
                }
            }
        }

        Files.createDirectories(destNio)
        recurse(src, destNio)
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
