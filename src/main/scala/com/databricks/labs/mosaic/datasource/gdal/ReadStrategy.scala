package com.databricks.labs.mosaic.datasource.gdal

import com.databricks.labs.mosaic._
import org.apache.hadoop.fs.{FileStatus, FileSystem}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

trait ReadStrategy extends Serializable {

    def getSchema(options: Map[String, String], files: Seq[FileStatus], parentSchema: StructType): StructType

    def read(
        status: FileStatus,
        fs: FileSystem,
        requiredSchema: StructType,
        options: Map[String, String]
    ): Iterator[InternalRow]

}

object ReadStrategy {

    def getReader(options: Map[String, String]): ReadStrategy = {
        val readStrategy = options.getOrElse(MOSAIC_RASTER_READ_STRATEGY, MOSAIC_RASTER_READ_IN_MEMORY)

        readStrategy match {
            case MOSAIC_RASTER_READ_IN_MEMORY  => ReadInMemory
            case MOSAIC_RASTER_READ_AS_PATH    => ReadAsPath
            case MOSAIC_RASTER_RE_TILE_ON_READ => ReTileOnRead
            case _                             => ReadInMemory
        }

    }

}
