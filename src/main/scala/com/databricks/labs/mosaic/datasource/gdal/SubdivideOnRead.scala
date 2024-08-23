package com.databricks.labs.mosaic.datasource.gdal

import com.databricks.labs.mosaic.{RASTER_DRIVER_KEY, RASTER_PARENT_PATH_KEY, RASTER_PATH_KEY, RASTER_SUBDATASET_NAME_KEY}
import com.databricks.labs.mosaic.core.index.{IndexSystem, IndexSystemFactory}
import com.databricks.labs.mosaic.core.raster.gdal.RasterGDAL
import com.databricks.labs.mosaic.core.raster.io.RasterIO.identifyDriverNameFromRawPath
import com.databricks.labs.mosaic.core.raster.operator.retile.BalancedSubdivision
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.datasource.Utils
import com.databricks.labs.mosaic.datasource.gdal.GDALFileFormat._
import com.databricks.labs.mosaic.functions.ExprConfig
import com.databricks.labs.mosaic.utils.PathUtils
import org.apache.hadoop.fs.{FileStatus, FileSystem}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

import scala.util.Try

/** An object defining the retiling read strategy for the GDAL file format. */
object SubdivideOnRead extends ReadStrategy {

    val tileDataType: DataType = StringType

    // noinspection DuplicatedCode
    /**
     * Returns the schema of the GDAL file format.
     * @note
     *   Different read strategies can have different schemas. This is because
     *   the schema is defined by the read strategy. For retiling we always use
     *   checkpoint location. In this case rasters are stored off spark rows.
     *   If you need the tiles in memory please load them from path stored in
     *   the tile returned by the reader.
     *
     * @param options
     *   Options passed to the reader.
     * @param files
     *   List of files to read.
     * @param parentSchema
     *   Parent schema.
     * @param sparkSession
     *   Spark session.
     *
     * @return
     *   Schema of the GDAL file format.
     */
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
            // Note that for retiling we always use checkpoint location.
            // In this case rasters are stored off spark rows.
            // If you need the tiles in memory please load them from path stored in the tile returned by the reader.
            .add(StructField(TILE, RasterTileType(indexSystem.getCellIdDataType, tileDataType, useCheckpoint = true), nullable = false))
    }

    /**
     * Reads the content of the file.
     *
     * @param status
     *   File status.
     * @param fs
     *   File system.
     * @param requiredSchema
     *   Required schema.
     * @param options
     *   Options passed to the reader.
     * @param indexSystem
     *   Index system.
     * @param exprConfigOpt
     *   Option [[ExprConfig]].
     * @return
     *   Iterator of internal rows.
     */
    override def read(
                         status: FileStatus,
                         fs: FileSystem,
                         requiredSchema: StructType,
                         options: Map[String, String],
                         indexSystem: IndexSystem,
                         exprConfigOpt: Option[ExprConfig]
                     ): Iterator[InternalRow] = {

        val inPath = status.getPath.toString
        val uuid = getUUID(status)
        val sizeInMB = options.getOrElse("sizeInMB", "16").toInt
        val uriDeepCheck = Try(exprConfigOpt.get.isUriDeepCheck).getOrElse(false)
        val uriGdalOpt = PathUtils.parseGdalUriOpt(inPath, uriDeepCheck)
        val driverName = options.get("driverName") match {
            case Some(name) if name.nonEmpty => name
            case _ => identifyDriverNameFromRawPath(inPath, uriGdalOpt)
        }
        val tmpPath = PathUtils.copyCleanPathToTmpWithRetry(inPath, exprConfigOpt, retries = 5)
        val createInfo = Map(
            RASTER_PATH_KEY -> tmpPath,
            RASTER_PARENT_PATH_KEY -> inPath,
            RASTER_DRIVER_KEY -> driverName,
            RASTER_SUBDATASET_NAME_KEY -> options.getOrElse("subdatasetName", "") // <- SUBDATASET HERE (PRE)!
        )
        val tiles = localSubdivide(createInfo, sizeInMB, exprConfigOpt)
        val rows = tiles.map(tile => {
            tile.finalizeTile(toFuse = true)  // <- raster written to configured checkpoint
            val raster = tile.raster

            // Clear out subset name on retile (subdivide)
            // - this is important to allow future loads to not try the path
            // - while subdivide should not be allowed for zips, testing just in case
            //raster.updateSubsetName(options.getOrElse("subdatasetName", "")) // <- SUBDATASET HERE (POST)!
            val trimmedSchema = StructType(requiredSchema.filter(field => field.name != TILE))
            val fields = trimmedSchema.fieldNames.map {
                case PATH              => status.getPath.toString
                case MODIFICATION_TIME => status.getModificationTime
                case UUID              => uuid
                case X_SIZE            => raster.xSize
                case Y_SIZE            => raster.ySize
                case BAND_COUNT        => raster.numBands
                case METADATA          => raster.metadata
                case SUBDATASETS       => raster.subdatasets
                case SRID              => raster.SRID
                case LENGTH            => raster.getMemSize
                case other             => throw new RuntimeException(s"Unsupported field name: $other")
            }
            val row = Utils.createRow(fields ++ Seq(
                tile
                    .formatCellId(indexSystem)
                    .serialize(tileDataType, doDestroy = true, exprConfigOpt)
            ))

            row
        })

        rows.iterator
    }

    /**
     * Subdivides a tile into tiles of a given size.
     *
     * @param createInfo
     *   Map with various KVs
     * @param sizeInMB
     *   Size of the tiles in MB.
     * @param exprConfigOpt
     *   Option [[ExprConfig]].
     * @return
     *  A tuple of the tile and the tiles.
     */
    def localSubdivide(
                          createInfo: Map[String, String],
                          sizeInMB: Int,
                          exprConfigOpt: Option[ExprConfig]
                      ): Seq[RasterTile] = {

        var raster = RasterGDAL(createInfo, exprConfigOpt).tryInitAndHydrate()
        var inTile = new RasterTile(null, raster, tileDataType)
        val tiles = BalancedSubdivision.splitRaster(inTile, sizeInMB, exprConfigOpt)

        inTile.flushAndDestroy()
        inTile = null
        raster = null

        tiles
    }

}
