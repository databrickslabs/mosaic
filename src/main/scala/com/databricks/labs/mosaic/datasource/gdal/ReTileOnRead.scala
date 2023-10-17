package com.databricks.labs.mosaic.datasource.gdal

import com.databricks.labs.mosaic.core.index.{IndexSystem, IndexSystemFactory}
import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.core.raster.api.RasterAPI
import com.databricks.labs.mosaic.core.raster.gdal_raster.{MosaicRasterGDAL, RasterCleaner}
import com.databricks.labs.mosaic.core.raster.operator.retile.BalancedSubdivision
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.datasource.Utils
import com.databricks.labs.mosaic.datasource.gdal.GDALFileFormat._
import com.databricks.labs.mosaic.utils.PathUtils
import org.apache.hadoop.fs.{FileStatus, FileSystem}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

object ReTileOnRead extends ReadStrategy {

    // noinspection DuplicatedCode
    override def getSchema(
        options: Map[String, String],
        files: Seq[FileStatus],
        parentSchema: StructType,
        sparkSession: SparkSession
    ): StructType = {
        val trimmedSchema = parentSchema.filter(field => field.name != CONTENT && field.name != LENGTH)
        val indexSystem = IndexSystemFactory.getIndexSystem(sparkSession)
        StructType(trimmedSchema)
            .add(StructField(UUID, LongType, nullable = false))
            .add(StructField(X_SIZE, IntegerType, nullable = false))
            .add(StructField(Y_SIZE, IntegerType, nullable = false))
            .add(StructField(BAND_COUNT, IntegerType, nullable = false))
            .add(StructField(METADATA, MapType(StringType, StringType), nullable = false))
            .add(StructField(SUBDATASETS, MapType(StringType, StringType), nullable = false))
            .add(StructField(SRID, IntegerType, nullable = false))
            .add(StructField(LENGTH, LongType, nullable = false))
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
        val uuid = getUUID(status)
        val sizeInMB = options.getOrElse("sizeInMB", "16").toInt

        val (raster, tiles) = localSubdivide(inPath, sizeInMB)

        val rows = tiles.map(tile => {
            val trimmedSchema = StructType(requiredSchema.filter(field => field.name != TILE))
            val fields = trimmedSchema.fieldNames.map {
                case PATH              => status.getPath.toString
                case MODIFICATION_TIME => status.getModificationTime
                case UUID              => uuid
                case X_SIZE            => tile.raster.xSize
                case Y_SIZE            => tile.raster.ySize
                case BAND_COUNT        => tile.raster.numBands
                case METADATA          => tile.raster.metadata
                case SUBDATASETS       => tile.raster.subdatasets
                case SRID              => tile.raster.SRID
                case LENGTH            => tile.raster.getMemSize
                case other             => throw new RuntimeException(s"Unsupported field name: $other")
            }
            // Writing to bytes is destructive so we delay reading content and content length until the last possible moment
            val row = Utils.createRow(fields ++ Seq(tile.formatCellId(indexSystem).serialize(rasterAPI)))
            RasterCleaner.dispose(tile)
            row
        })

        RasterCleaner.dispose(raster)
        rows.iterator
    }

    def localSubdivide(inPath: String, sizeInMB: Int): (MosaicRaster, Seq[MosaicRasterTile]) = {
        val localCopy = PathUtils.copyToTmp(inPath)
        val raster = MosaicRasterGDAL.readRaster(localCopy, inPath)
        val inTile = MosaicRasterTile(null, raster, inPath, raster.getDriversShortName)
        val tiles = BalancedSubdivision.splitRaster(inTile, sizeInMB)
        (raster, tiles)
    }

}
