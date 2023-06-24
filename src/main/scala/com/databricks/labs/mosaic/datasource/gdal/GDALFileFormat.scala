package com.databricks.labs.mosaic.datasource.gdal

import com.databricks.labs.mosaic.GDAL
import com.google.common.io.{ByteStreams, Closeables}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.orc.util.Murmur3
import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.binaryfile.BinaryFileFormat
import org.apache.spark.sql.execution.datasources.{OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

import java.net.URI
import java.sql.Timestamp
import java.util.Locale

class GDALFileFormat extends BinaryFileFormat {

    import GDALFileFormat._

    override def inferSchema(sparkSession: SparkSession, options: Map[String, String], files: Seq[FileStatus]): Option[StructType] = {
        GDAL.enable()

        val reader = ReadStrategy.getReader(options)
        val schema = super
            .inferSchema(sparkSession, options, files)
            .map(reader.getSchema(options, files, _))

        schema
    }

    override def prepareWrite(
        sparkSession: SparkSession,
        job: Job,
        options: Map[String, String],
        dataSchema: StructType
    ): OutputWriterFactory = {
        throw new Error("Writing to GDALFileFormat is not supported.")
    }

    override def isSplitable(
        sparkSession: SparkSession,
        options: Map[String, String],
        path: org.apache.hadoop.fs.Path
    ): Boolean = false

    override def shortName(): String = GDAL_BINARY_FILE

    override def buildReader(
        sparkSession: SparkSession,
        dataSchema: StructType,
        partitionSchema: StructType,
        requiredSchema: StructType,
        filters: Seq[Filter],
        options: Map[String, String],
        hadoopConf: org.apache.hadoop.conf.Configuration
    ): PartitionedFile => Iterator[org.apache.spark.sql.catalyst.InternalRow] = {
        GDAL.enable()

        val broadcastedHadoopConf = sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
        val filterFuncs = filters.flatMap(createFilterFunction)
        val maxLength = sparkSession.conf.get("spark.sql.sources.binaryFile.maxLength", Int.MaxValue.toString).toInt

        val reader = ReadStrategy.getReader(options)

        file: PartitionedFile => {
            GDAL.enable()
            val path = new Path(new URI(file.filePath))
            val fs = path.getFileSystem(broadcastedHadoopConf.value.value)
            val status = fs.getFileStatus(path)

            if (status.getLen > maxLength) throw CantReadBytesException(maxLength, status)

            if (filterFuncs.forall(_.apply(status)) && isAllowedExtension(status, options)) {
                reader.read(status, fs, requiredSchema, options)
            } else {
                Iterator.empty
            }
        }

    }

}

object GDALFileFormat {

    val GDAL_BINARY_FILE = "gdal"
    val PATH = "path"
    val LENGTH = "length"
    val MODIFICATION_TIME = "modificationTime"
    val RASTER = "raster"
    val CONTENT = "content"
    val X_SIZE = "x_size"
    val Y_SIZE = "y_size"
    val X_OFFSET = "x_offset"
    val Y_OFFSET = "y_offset"
    val BAND_COUNT = "bandCount"
    val METADATA = "metadata"
    val SUBDATASETS: String = "subdatasets"
    val SRID = "srid"
    val UUID = "uuid"

    def CantReadBytesException(maxLength: Long, status: FileStatus): SparkException =
        new SparkException(
          s"Can't read binary files bigger than $maxLength bytes. " +
              s"File ${status.getPath} is ${status.getLen} bytes"
        )

    def getUUID(status: FileStatus): Long = {
        val uuid = Murmur3.hash64(
          status.getPath.toString.getBytes("UTF-8") ++
              status.getLen.toString.getBytes("UTF-8") ++
              status.getModificationTime.toString.getBytes("UTF-8")
        )
        uuid
    }

    // noinspection UnstableApiUsage
    def readContent(fs: FileSystem, status: FileStatus): Array[Byte] = {
        val stream = fs.open(status.getPath)
        try { ByteStreams.toByteArray(stream) }
        finally { Closeables.close(stream, true) }
    }

    def isAllowedExtension(status: FileStatus, options: Map[String, String]): Boolean = {
        val allowedExtensions = options.getOrElse("extensions", "*").split(";").map(_.trim.toLowerCase(Locale.ROOT))
        val fileExtension = status.getPath.getName.toLowerCase(Locale.ROOT)
        allowedExtensions.contains("*") || allowedExtensions.exists(fileExtension.endsWith)
    }

    private def createFilterFunction(filter: Filter): Option[FileStatus => Boolean] = {
        filter match {
            case And(left, right)                                     => (createFilterFunction(left), createFilterFunction(right)) match {
                    case (Some(leftPred), Some(rightPred)) => Some(s => leftPred(s) && rightPred(s))
                    case (Some(leftPred), None)            => Some(leftPred)
                    case (None, Some(rightPred))           => Some(rightPred)
                    case (None, None)                      => Some(_ => true)
                }
            case Or(left, right)                                      => (createFilterFunction(left), createFilterFunction(right)) match {
                    case (Some(leftPred), Some(rightPred)) => Some(s => leftPred(s) || rightPred(s))
                    case _                                 => Some(_ => true)
                }
            case Not(child)                                           => createFilterFunction(child) match {
                    case Some(pred) => Some(s => !pred(s))
                    case _          => Some(_ => true)
                }
            case LessThan(LENGTH, value: Long)                        => Some(_.getLen < value)
            case LessThanOrEqual(LENGTH, value: Long)                 => Some(_.getLen <= value)
            case GreaterThan(LENGTH, value: Long)                     => Some(_.getLen > value)
            case GreaterThanOrEqual(LENGTH, value: Long)              => Some(_.getLen >= value)
            case EqualTo(LENGTH, value: Long)                         => Some(_.getLen == value)
            case LessThan(MODIFICATION_TIME, value: Timestamp)        => Some(_.getModificationTime < value.getTime)
            case LessThanOrEqual(MODIFICATION_TIME, value: Timestamp) => Some(_.getModificationTime <= value.getTime)
            case GreaterThan(MODIFICATION_TIME, value: Timestamp)     => Some(_.getModificationTime > value.getTime)
            case GreaterThanOrEqual(MODIFICATION_TIME, value: Timestamp) => Some(_.getModificationTime >= value.getTime)
            case EqualTo(MODIFICATION_TIME, value: Timestamp)            => Some(_.getModificationTime == value.getTime)
            case _                                                       => None
        }
    }

}
