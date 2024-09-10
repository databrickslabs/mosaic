package com.databricks.labs.mosaic.datasource.gdal

import com.databricks.labs.mosaic.MOSAIC_RASTER_READ_IN_MEMORY
import com.databricks.labs.mosaic.core.index.IndexSystemFactory
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.functions.ExprConfig
import com.databricks.labs.mosaic.gdal.MosaicGDAL
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
import scala.util.Try

/** A file format for reading binary files using GDAL. */
class GDALFileFormat extends BinaryFileFormat {

    import GDALFileFormat._

    /**
      * Infer schema for the tile file.
      * @param sparkSession
      *   Spark session.
      * @param options
      *   Reading options.
      * @param files
      *   List of files.
      * @return
      *   An instance of [[StructType]].
      */
    override def inferSchema(sparkSession: SparkSession, options: Map[String, String], files: Seq[FileStatus]): Option[StructType] = {
        GDAL.enable(sparkSession)

        val reader = ReadStrategy.getReader(options)
        val schema = super
            .inferSchema(sparkSession, options, files)
            .map(reader.getSchema(options, files, _, sparkSession))

        schema
    }

    /**
      * Prepare write is not supported.
      * @param sparkSession
      *   Spark session.
      * @param job
      *   Job.
      * @param options
      *   Writing options.
      * @param dataSchema
      *   Data schema.
      * @return
      *   An instance of [[OutputWriterFactory]].
      */
    override def prepareWrite(
        sparkSession: SparkSession,
        job: Job,
        options: Map[String, String],
        dataSchema: StructType
    ): OutputWriterFactory = {
        throw new Error("Writing to GDALFileFormat is not supported.")
    }

    /**
      * Indicates whether the file format is splittable.
      * @param sparkSession
      *   Spark session.
      * @param options
      *   Reading options.
      * @param path
      *   Path.
      * @return
      *   True if the file format is splittable, false otherwise. Always false
      *   for GDAL.
      */
    override def isSplitable(
        sparkSession: SparkSession,
        options: Map[String, String],
        path: org.apache.hadoop.fs.Path
    ): Boolean = false

    override def shortName(): String = GDAL_FILE_FORMAT

    /**
      * Build a reader for the file format.
      * @param sparkSession
      *   Spark session.
      * @param dataSchema
      *   Data schema.
      * @param partitionSchema
      *   Partition schema.
      * @param requiredSchema
      *   Required schema.
      * @param filters
      *   Filters.
      * @param options
      *   Reading options.
      * @param hadoopConf
      *   Hadoop configuration.
      * @return
      *   A function that takes a [[PartitionedFile]] and returns an iterator of
      *   [[org.apache.spark.sql.catalyst.InternalRow]].
      */
    override def buildReader(
        sparkSession: SparkSession,
        dataSchema: StructType,
        partitionSchema: StructType,
        requiredSchema: StructType,
        filters: Seq[Filter],
        options: Map[String, String],
        hadoopConf: org.apache.hadoop.conf.Configuration
    ): PartitionedFile => Iterator[org.apache.spark.sql.catalyst.InternalRow] = {
        // Suitable on the driver
        MosaicGDAL.enableGDAL(sparkSession)

        // Identify the reader to use for the file format.
        // GDAL supports multiple reading strategies.
        val reader = ReadStrategy.getReader(options)

        val indexSystem = IndexSystemFactory.getIndexSystem(sparkSession)
        val supportedExtensions = options.getOrElse("extensions", "*").split(";").map(_.trim.toLowerCase(Locale.ROOT))
        val broadcastedHadoopConf = sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
        val filterFuncs = filters.flatMap(createFilterFunction)

        file: PartitionedFile => {
            val path = new Path(new URI(file.filePath.toString()))
            val fs = path.getFileSystem(broadcastedHadoopConf.value.value)
            val status = fs.getFileStatus(path)

            if (supportedExtensions.contains("*") || supportedExtensions.exists(status.getPath.getName.toLowerCase(Locale.ROOT).endsWith)) {
                if (filterFuncs.forall(_.apply(status)) && isAllowedExtension(status, options)) {
                    reader.read(status, fs, requiredSchema, options, indexSystem)
                } else {
                    Iterator.empty
                }
            } else {
                Iterator.empty
            }
        }

    }

}

object GDALFileFormat {

    val GDAL_FILE_FORMAT = "gdal"
    val PATH = "path"
    val LENGTH = "length"
    val MODIFICATION_TIME = "modificationTime"
    val TILE = "tile"
    val CONTENT = "content"
    val X_SIZE = "x_size"
    val Y_SIZE = "y_size"
    val BAND_COUNT = "bandCount"
    val METADATA = "metadata"
    val SUBDATASETS: String = "subdatasets"
    val SRID = "srid"
    val UUID = "uuid"

    /**
      * Creates an exception for when the file is too big to read.
      * @param maxLength
      *   Maximum length.
      * @param status
      *   File status.
      * @return
      *   An instance of [[SparkException]].
      */
    def CantReadBytesException(maxLength: Long, status: FileStatus): SparkException =
        new SparkException(
          s"Can't read binary files bigger than $maxLength bytes. " +
              s"File ${status.getPath} is ${status.getLen} bytes"
        )

    /**
      * Generates a UUID for the file.
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

    // noinspection UnstableApiUsage
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

    /**
      * Indicates whether the file extension is allowed.
      * @param status
      *   File status.
      * @param options
      *   Reading options.
      * @return
      *   True if the file extension is allowed, false otherwise.
      */
    def isAllowedExtension(status: FileStatus, options: Map[String, String]): Boolean = {
        val allowedExtensions = options.getOrElse("extensions", "*").split(";").map(_.trim.toLowerCase(Locale.ROOT))
        val fileExtension = status.getPath.getName.toLowerCase(Locale.ROOT)
        allowedExtensions.contains("*") || allowedExtensions.exists(fileExtension.endsWith)
    }

    /**
      * Creates a filter function for the file.
      * @param filter
      *   Filter.
      * @return
      *   An instance of [[FileStatus]] => [[Boolean]].
      */
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
