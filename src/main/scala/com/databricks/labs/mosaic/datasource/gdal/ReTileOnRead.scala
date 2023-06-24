package com.databricks.labs.mosaic.datasource.gdal

import com.databricks.labs.mosaic.core.raster.gdal_raster.{MosaicRasterGDAL, RasterCleaner}
import com.databricks.labs.mosaic.core.raster.operator.retile.BalancedSubdivision
import com.databricks.labs.mosaic.datasource.Utils
import com.databricks.labs.mosaic.datasource.gdal.GDALFileFormat._
import com.databricks.labs.mosaic.utils.PathUtils
import org.apache.hadoop.fs.{FileStatus, FileSystem}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

object ReTileOnRead extends ReadStrategy {

    // 16 MB in bytes, the desired maximum size of a tile
    // 1GB file will be split into 64 tiles if the ration is maintained
    // if the ratio is not maintained, the tile will be split one more time
    // resulting in 128 tiles
    val MB16 = 16777216

    // noinspection DuplicatedCode
    override def getSchema(options: Map[String, String], files: Seq[FileStatus], parentSchema: StructType): StructType = {
        val trimmedSchema = parentSchema.filter(field => field.name != CONTENT && field.name != LENGTH)
        StructType(trimmedSchema)
            .add(StructField(UUID, LongType, nullable = false))
            .add(StructField(X_SIZE, IntegerType, nullable = false))
            .add(StructField(Y_SIZE, IntegerType, nullable = false))
            .add(StructField(BAND_COUNT, IntegerType, nullable = false))
            .add(StructField(METADATA, MapType(StringType, StringType), nullable = false))
            .add(StructField(SUBDATASETS, MapType(StringType, StringType), nullable = false))
            .add(StructField(SRID, IntegerType, nullable = false))
            .add(StructField(RASTER, BinaryType, nullable = false))
            .add(StructField(LENGTH, LongType, nullable = false))
    }

    override def read(
        status: FileStatus,
        fs: FileSystem,
        requiredSchema: StructType,
        options: Map[String, String]
    ): Iterator[InternalRow] = {
        val localCopy = PathUtils.copyToTmp(status.getPath.toString)
        val raster = MosaicRasterGDAL.readRaster(localCopy)
        val uuid = getUUID(status)

        val size = status.getLen
        val numSplits = Math.ceil(size / MB16).toInt
        val tiles = BalancedSubdivision.splitRaster(raster, numSplits)

        val rows = tiles.map(tile => {
            val trimmedSchema = StructType(requiredSchema.filter(field => field.name != RASTER && field.name != LENGTH))
            val fields = trimmedSchema.fieldNames.map {
                case PATH              => status.getPath.toString
                case MODIFICATION_TIME => status.getModificationTime
                case UUID              => uuid
                case X_SIZE            => tile.xSize
                case Y_SIZE            => tile.ySize
                case BAND_COUNT        => tile.numBands
                case METADATA          => tile.metadata
                case SUBDATASETS       => tile.subdatasets
                case SRID              => tile.SRID
                case other             => throw new RuntimeException(s"Unsupported field name: $other")
            }
            // Writing to bytes is destructive so we delay reading content and content length until the last possible moment
            val contentBytes = tile.writeToBytes()
            val contentFields = Seq(contentBytes, contentBytes.length.toLong)
            val row = Utils.createRow(fields ++ contentFields)
            RasterCleaner.dispose(tile)
            row
        })

        RasterCleaner.dispose(raster)
        rows.iterator
    }

}
