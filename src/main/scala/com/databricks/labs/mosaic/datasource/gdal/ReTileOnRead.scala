package com.databricks.labs.mosaic.datasource.gdal

import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.core.raster.io.RasterCleaner
import com.databricks.labs.mosaic.core.raster.operator.retile.BalancedSubdivision
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.utils.{HadoopUtils, PathUtils, ReaderUtils}
import org.apache.hadoop.fs.{FileStatus, FileSystem}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

/** An object defining the retiling read strategy for the GDAL file format. */
object ReTileOnRead extends ReadStrategy {

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
        ReadAsPath.getSchema(options, files, parentSchema, sparkSession)
    }

    /**
      * Reads the content of the file.
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
      *
      * @return
      *   Iterator of internal rows.
      */
    override def read(
        status: FileStatus,
        fs: FileSystem,
        requiredSchema: StructType,
        options: Map[String, String],
        indexSystem: IndexSystem
    ): Iterator[InternalRow] = {
        val uuid = HadoopUtils.getUUID(status)
        val sizeInMB = options.getOrElse("sizeInMB", "16").toInt
        val inPath = status.getPath.toString
        val hconf = new SerializableConfiguration(fs.getConf)

        // Hadoop copy to local to account for the Volumes
        val tmpPath1 = HadoopUtils.copyToLocalTmp(inPath, hconf)
        // After copying to local we can proceed as if the file was never on the volume
        // This was done to avoid redoing the logic for subdatasets via Hadoop file wrangling
        // for some reason both returned the same path ????
        val tmpPath2 = ReaderUtils.asTmpRaster(tmpPath1, options, hconf)

        val tiles = localSubdivide(tmpPath2, inPath, sizeInMB)

        val rows = tiles.map(tile => ReadAsPath.createRow(status, tile, uuid, requiredSchema, indexSystem, hconf))

        // Both tmp files are local and can be deleted
        // here using PathUtils is safe, and it accounts for subdatasets complications
        PathUtils.cleanUpPath(tmpPath1)
        PathUtils.cleanUpPath(tmpPath2)

        rows.iterator
    }

    /**
      * Subdivides a raster into tiles of a given size.
      * @param inPath
      *   Path to the raster.
      * @param sizeInMB
      *   Size of the tiles in MB.
      *
      * @return
      *   A tuple of the raster and the tiles.
      */
    def localSubdivide(inPath: String, parentPath: String, sizeInMB: Int): Seq[MosaicRasterTile] = {
        val cleanPath = PathUtils.getCleanPath(inPath)
        val createInfo = Map("path" -> cleanPath, "parentPath" -> parentPath)
        val raster = MosaicRasterGDAL.readRaster(createInfo)
        val inTile = new MosaicRasterTile(null, raster)
        val tiles = BalancedSubdivision.splitRaster(inTile, sizeInMB)
        RasterCleaner.dispose(raster)
        RasterCleaner.dispose(inTile)
        tiles
    }

}
