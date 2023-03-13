package com.databricks.labs.mosaic.datasource

import com.databricks.labs.mosaic.core.raster.MosaicRasterGDAL
import com.databricks.labs.mosaic.GDAL
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
  * A base Spark SQL data source for reading GDAL raster data sources. It reads
  * metadata of the raster and exposes the direct paths for the raster files.
  */
class GDALFileFormat extends FileFormat with DataSourceRegister with Serializable {

    import GDALFileFormat._

    override def shortName(): String = "gdal"

    override def inferSchema(
        sparkSession: SparkSession,
        options: Map[String, String],
        files: Seq[FileStatus]
    ): Option[StructType] = {
        inferSchemaImpl()
    }

    override def isSplitable(
        sparkSession: SparkSession,
        options: Map[String, String],
        path: org.apache.hadoop.fs.Path
    ): Boolean = false

    override def buildReader(
        sparkSession: SparkSession,
        dataSchema: StructType,
        partitionSchema: StructType,
        requiredSchema: StructType,
        filters: Seq[Filter],
        options: Map[String, String],
        hadoopConf: Configuration
    ): PartitionedFile => Iterator[InternalRow] = {
        val driverName = options.getOrElse("driverName", "")
        buildReaderImpl(driverName)
    }

    override def prepareWrite(
        sparkSession: SparkSession,
        job: Job,
        options: Map[String, String],
        dataSchema: StructType
    ): OutputWriterFactory = throw new Error("Not implemented")

}

object GDALFileFormat {

    /**
      * Returns the supported file extension for the driver name.
      *
      * @param driverName
      *   the GDAL driver name
      * @return
      *   the file extension
      */
    def getFileExtension(driverName: String): String = {
        // Not a complete list of GDAL drivers
        driverName match {
            case "GTiff"       => "tif"
            case "HDF4"        => "hdf"
            case "HDF5"        => "hdf"
            case "JP2ECW"      => "jp2"
            case "JP2KAK"      => "jp2"
            case "JP2MrSID"    => "jp2"
            case "JP2OpenJPEG" => "jp2"
            case "NetCDF"      => "nc"
            case "PDF"         => "pdf"
            case "PNG"         => "png"
            case "VRT"         => "vrt"
            case "XPM"         => "xpm"
            case "COG"         => "tif"
            case "GRIB"        => "grib"
            case "Zarr"        => "zarr"
            case _             => ""
        }
    }

    /**
      * Creates a Spark SQL row from a sequence of values.
      *
      * @param values
      *   sequence of values.
      * @return
      *   Spark SQL row.
      */
    def createRow(values: Seq[Any]): InternalRow = {
        import com.databricks.labs.mosaic.expressions.raster
        InternalRow.fromSeq(
          values.map {
              case null           => null
              case b: Array[Byte] => b
              case v: Array[_]    => new GenericArrayData(v)
              case m: Map[_, _]   => raster.buildMapString(m.map { case (k, v) => (k.toString, v.toString) })
              case s: String      => UTF8String.fromString(s)
              case v              => v
          }
        )
    }

    /** GDAL readers have fixed schema. */
    def inferSchemaImpl(): Option[StructType] = {

        Some(
          StructType(
            Array(
              StructField("path", StringType, nullable = false),
              StructField("ySize", IntegerType, nullable = false),
              StructField("xSize", IntegerType, nullable = false),
              StructField("bandCount", IntegerType, nullable = false),
              StructField("metadata", MapType(StringType, StringType), nullable = false),
              StructField("subdatasets", MapType(StringType, StringType), nullable = false),
              StructField("srid", IntegerType, nullable = false),
              StructField("proj4Str", StringType, nullable = false)
            )
          )
        )

    }

    def buildReaderImpl(
        driverName: String
    ): PartitionedFile => Iterator[InternalRow] = { file: PartitionedFile =>
        {
            GDAL.enable()
            val path = file.filePath.replace("dbfs:/", "/dbfs/").replace("file:/", "/")
            val extension = file.filePath.split("\\.").last
            if (extension == getFileExtension(driverName) || extension == "zip") {
                val raster = MosaicRasterGDAL.readRaster(path)
                val ySize = raster.ySize
                val xSize = raster.xSize
                val bandCount = raster.numBands
                val metadata = raster.metadata
                val subdatasets = raster.subdatasets
                val srid = raster.SRID
                val proj4Str = raster.proj4String
                val row = createRow(Seq(path, ySize, xSize, bandCount, metadata, subdatasets, srid, proj4Str))
                Seq(row).iterator
            } else {
                Seq.empty[InternalRow].iterator
            }
        }
    }

}
