package com.databricks.labs.mosaic.datasource.gdal

import com.databricks.labs.mosaic.core.raster.gdal_raster.MosaicRasterGDAL
import com.databricks.labs.mosaic.datasource.Utils
import com.databricks.labs.mosaic.datasource.gdal.GDALFileFormat._
import org.apache.hadoop.fs.{FileStatus, FileSystem}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._

object ReadAsPath extends ReadStrategy {

    // noinspection DuplicatedCode
    override def getSchema(options: Map[String, String], files: Seq[FileStatus], parentSchema: StructType): StructType = {
        StructType(parentSchema.filter(_.name != CONTENT))
            .add(StructField(UUID, LongType, nullable = false))
            .add(StructField(X_SIZE, IntegerType, nullable = false))
            .add(StructField(Y_SIZE, IntegerType, nullable = false))
            .add(StructField(BAND_COUNT, IntegerType, nullable = false))
            .add(StructField(METADATA, MapType(StringType, StringType), nullable = false))
            .add(StructField(SUBDATASETS, MapType(StringType, StringType), nullable = false))
            .add(StructField(SRID, IntegerType, nullable = false))
    }

    override def read(
        status: FileStatus,
        fs: FileSystem,
        requiredSchema: StructType,
        options: Map[String, String]
    ): Iterator[InternalRow] = {
        val raster = MosaicRasterGDAL.readRaster(status.getPath.toString)
        val uuid = getUUID(status)

        val fields = requiredSchema.fieldNames.map {
            case PATH              => status.getPath.toString
            case LENGTH            => status.getLen
            case MODIFICATION_TIME => DateTimeUtils.millisToMicros(status.getModificationTime)
            case RASTER            => status.getPath.toString
            case UUID              => uuid
            case X_SIZE            => raster.xSize
            case Y_SIZE            => raster.ySize
            case BAND_COUNT        => raster.numBands
            case METADATA          => raster.metadata
            case SUBDATASETS       => raster.subdatasets
            case SRID              => raster.SRID
            case other             => throw new RuntimeException(s"Unsupported field name: $other")
        }
        val row = Utils.createRow(fields)
        Seq(row).iterator

    }

}
