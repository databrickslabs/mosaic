package com.databricks.labs.mosaic.datasource.gdal

import com.databricks.labs.mosaic.core.index.{IndexSystem, IndexSystemFactory}
import com.databricks.labs.mosaic.core.raster.api.RasterAPI
import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.core.raster.io.RasterCleaner
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.datasource.Utils
import com.databricks.labs.mosaic.datasource.gdal.GDALFileFormat._
import org.apache.hadoop.fs.{FileStatus, FileSystem}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object ReadInMemory extends ReadStrategy {

    // noinspection DuplicatedCode
    override def getSchema(
        options: Map[String, String],
        files: Seq[FileStatus],
        parentSchema: StructType,
        sparkSession: SparkSession
    ): StructType = {
        val indexSystem = IndexSystemFactory.getIndexSystem(sparkSession)
        StructType(parentSchema.filter(_.name != CONTENT))
            .add(StructField(UUID, LongType, nullable = false))
            .add(StructField(X_SIZE, IntegerType, nullable = false))
            .add(StructField(Y_SIZE, IntegerType, nullable = false))
            .add(StructField(BAND_COUNT, IntegerType, nullable = false))
            .add(StructField(METADATA, MapType(StringType, StringType), nullable = false))
            .add(StructField(SUBDATASETS, MapType(StringType, StringType), nullable = false))
            .add(StructField(SRID, IntegerType, nullable = false))
            .add(StructField(TILE, RasterTileType(indexSystem.getCellIdDataType), nullable = false))
    }

    override def read(
        status: FileStatus,
        fs: FileSystem,
        requiredSchema: StructType,
        options: Map[String, String],
        indexSystem: IndexSystem,
        rasterAPI: RasterAPI
    ): Iterator[InternalRow] = {
        val inPath = status.getPath.toString
        val driverShortName = MosaicRasterGDAL.indentifyDriver(inPath)
        val contentBytes: Array[Byte] = readContent(fs, status)
        val raster = MosaicRasterGDAL.readRaster(contentBytes, inPath, driverShortName)
        val uuid = getUUID(status)

        val fields = requiredSchema.fieldNames.filter(_ != TILE).map {
            case PATH              => status.getPath.toString
            case LENGTH            => status.getLen
            case MODIFICATION_TIME => status.getModificationTime
            case UUID              => uuid
            case X_SIZE            => raster.xSize
            case Y_SIZE            => raster.ySize
            case BAND_COUNT        => raster.numBands
            case METADATA          => raster.metadata
            case SUBDATASETS       => raster.subdatasets
            case SRID              => raster.SRID
            case other             => throw new RuntimeException(s"Unsupported field name: $other")
        }
        val rasterTileSer = InternalRow.fromSeq(
          Seq(-1L, contentBytes, UTF8String.fromString(inPath), UTF8String.fromString(driverShortName))
        )
        val row = Utils.createRow(
          fields ++ Seq(rasterTileSer)
        )
        RasterCleaner.dispose(raster)
        Seq(row).iterator
    }

}
