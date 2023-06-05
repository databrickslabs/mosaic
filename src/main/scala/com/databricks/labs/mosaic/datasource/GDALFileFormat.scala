package com.databricks.labs.mosaic.datasource

import com.databricks.labs.mosaic.core.raster.gdal_raster.MosaicRasterGDAL
import com.databricks.labs.mosaic.{GDAL, MOSAIC_RASTER_STORAGE_IN_MEMORY}
import com.google.common.io.{ByteStreams, Closeables}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.orc.util.Murmur3
import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.DateTimeUtils
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

        val storage = options.getOrElse("raster_storage", MOSAIC_RASTER_STORAGE_IN_MEMORY)

        super
            .inferSchema(sparkSession, options, files)
            // Drop CONTENT since RASTER will contain the CONTENT bytes if they are needed
            .map(schema => StructType(schema.filter(_.name != CONTENT)))
            .map(parentSchema =>
                parentSchema
                    .add(StructField(UUID, LongType, nullable = false))
                    .add(StructField(X_SIZE, IntegerType, nullable = false))
                    .add(StructField(Y_SIZE, IntegerType, nullable = false))
                    .add(StructField(BAND_COUNT, IntegerType, nullable = false))
                    .add(StructField(METADATA, MapType(StringType, StringType), nullable = false))
                    .add(StructField(SUBDATASETS, MapType(StringType, StringType), nullable = false))
                    .add(StructField(SRID, IntegerType, nullable = false))
                    .add(StructField(PROJ4_STR, StringType, nullable = false))
            )
            .map(schema =>
                if (storage == MOSAIC_RASTER_STORAGE_IN_MEMORY) schema.add(StructField(RASTER, BinaryType, nullable = false))
                else schema
            )
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

        file: PartitionedFile => {
            val path = new Path(new URI(file.filePath))
            val fs = path.getFileSystem(broadcastedHadoopConf.value.value)
            val status = fs.getFileStatus(path)

            val readRasterFlag = shouldReadRaster(requiredSchema)
            if (readRasterFlag && status.getLen > maxLength) throw CantReadBytesException(maxLength, status)

            var fields: Seq[Any] = Seq.empty[Any]
            val rasterStorage = options.getOrElse("raster_storage", MOSAIC_RASTER_STORAGE_IN_MEMORY)
            val isInMem = rasterStorage == MOSAIC_RASTER_STORAGE_IN_MEMORY

            if (filterFuncs.forall(_.apply(status)) && isAllowedExtension(status, options)) {

                val contentBytes: Array[Byte] = readContent(fs, status, readRasterFlag && isInMem)
                val raster =
                    if (isInMem) {
                        MosaicRasterGDAL.readRaster(contentBytes)
                    } else {
                        MosaicRasterGDAL.readRaster(status.getPath.toString)
                    }
                val uuid = Murmur3.hash64(
                  status.getPath.toString.getBytes("UTF-8") ++
                      status.getLen.toString.getBytes("UTF-8") ++
                      status.getModificationTime.toString.getBytes("UTF-8")
                )

                requiredSchema.fieldNames.foreach {
                    case PATH              => fields = fields :+ status.getPath.toString
                    case LENGTH            => fields = fields :+ status.getLen
                    case MODIFICATION_TIME => fields = fields :+ DateTimeUtils.millisToMicros(status.getModificationTime)
                    case RASTER            => if (isInMem) fields = fields :+ contentBytes else fields = fields :+ status.getPath.toString
                    case UUID              => fields = fields :+ uuid
                    case X_SIZE            => fields = fields :+ raster.xSize
                    case Y_SIZE            => fields = fields :+ raster.ySize
                    case BAND_COUNT        => fields = fields :+ raster.numBands
                    case METADATA          => fields = fields :+ raster.metadata
                    case SUBDATASETS       => fields = fields :+ raster.subdatasets
                    case SRID              => fields = fields :+ raster.SRID
                    case PROJ4_STR         => fields = fields :+ raster.proj4String
                    case other             => throw new RuntimeException(s"Unsupported field name: $other")
                }
                val row = Utils.createRow(fields)
                Seq(row).iterator
            } else {
                Iterator.empty
            }
        }

    }

}

object GDALFileFormat {

    private val GDAL_BINARY_FILE = "gdal"
    private val PATH = "path"
    private val LENGTH = "length"
    private val MODIFICATION_TIME = "modificationTime"
    private val RASTER = "raster"
    private val CONTENT = "content"
    private val X_SIZE = "xSize"
    private val Y_SIZE = "ySize"
    private val BAND_COUNT = "bandCount"
    private val METADATA = "metadata"
    val SUBDATASETS: String = "subdatasets"
    val SRID = "srid"
    private val PROJ4_STR = "proj4Str"
    val UUID = "uuid"

    private def CantReadBytesException(maxLength: Long, status: FileStatus) =
        new SparkException(
          s"Can't read binary files bigger than $maxLength bytes. " +
              s"File ${status.getPath} is ${status.getLen} bytes"
        )

    private def shouldReadRaster(requiredSchema: StructType): Boolean =
        requiredSchema.fieldNames.exists {
            case UUID | X_SIZE | Y_SIZE | BAND_COUNT | METADATA | SUBDATASETS | SRID | PROJ4_STR => true
            case _                                                                               => false
        }

    // noinspection UnstableApiUsage
    private def readContent(fs: FileSystem, status: FileStatus, readRasterFlag: Boolean): Array[Byte] = {
        if (readRasterFlag) {
            val stream = fs.open(status.getPath)
            try {
                ByteStreams.toByteArray(stream)
            } finally {
                Closeables.close(stream, true)
            }
        } else null
    }

    private def isAllowedExtension(status: FileStatus, options: Map[String, String]): Boolean = {
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
