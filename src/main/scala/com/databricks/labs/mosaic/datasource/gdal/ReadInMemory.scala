package com.databricks.labs.mosaic.datasource.gdal

import com.databricks.labs.mosaic.core.index.{IndexSystem, IndexSystemFactory}
import com.databricks.labs.mosaic.core.raster.gdal.RasterGDAL
import com.databricks.labs.mosaic.core.raster.io.RasterIO.identifyDriverNameFromRawPath
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.datasource.Utils
import com.databricks.labs.mosaic.datasource.gdal.GDALFileFormat._
import com.databricks.labs.mosaic.expressions.raster.buildMapString
import com.databricks.labs.mosaic.functions.ExprConfig
import com.databricks.labs.mosaic.utils.PathUtils
import org.apache.hadoop.fs.{FileStatus, FileSystem}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

/** An object defining the in memory read strategy for the GDAL file format. */
object ReadInMemory extends ReadStrategy {

    //serialize data type
    val tileDataType: DataType = BinaryType

    // noinspection DuplicatedCode
    /**
      * Returns the schema of the GDAL file format.
      * @note
      *   Different read strategies can have different schemas.
      *
      * @param options
      *   Options passed to the reader.
      * @param files
      *   List of files to read.
      * @param parentSchema
      *   Parent schema.
      * @param sparkSession
      *   Spark session.
      * @return
      *   Schema of the GDAL file format.
      */
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
            // Note, for in memory reads the rasters are stored in the tile.
            // For that we use Binary Columns.
            .add(StructField(TILE, RasterTileType(indexSystem.getCellIdDataType, tileDataType, useCheckpoint = false), nullable = false))
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
      * Options passed to the reader.
      * @param indexSystem
      * Index system.
      * @param exprConfig
      * [[ExprConfig]]
      * @return
      *   Iterator of internal rows.
      */
    override def read(
                         status: FileStatus,
                         fs: FileSystem,
                         requiredSchema: StructType,
                         options: Map[String, String],
                         indexSystem: IndexSystem,
                         exprConfig: ExprConfig
    ): Iterator[InternalRow] = {
        val inPath = status.getPath.toString
        val readPath = PathUtils.asFileSystemPath(inPath)
        val contentBytes: Array[Byte] = readContent(fs, status)
        val createInfo = Map(
            "path" -> readPath,
            "parentPath" -> inPath,
            "driver" -> identifyDriverNameFromRawPath(inPath)
        )
        val raster = RasterGDAL(createInfo, Option(exprConfig))
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
        val mapData = buildMapString(raster.getCreateInfo)
        val rasterTileSer = InternalRow.fromSeq(Seq(null, contentBytes, mapData))
        val row = Utils.createRow(fields ++ Seq(rasterTileSer))
        val rows = Seq(row)

        raster.flushAndDestroy()

        rows.iterator
    }

}
