package com.databricks.labs.mosaic.datasource.gdal

import com.databricks.labs.mosaic._
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.hadoop.fs.{FileStatus, FileSystem}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

/** A trait defining the read strategy for the GDAL file format. */
trait ReadStrategy extends Serializable {

    /** @return the ReadStrategy name implemented. */
    def getReadStrategy: String

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
      *
      * @return
      *   Schema of the GDAL file format.
      */
    def getSchema(options: Map[String, String], files: Seq[FileStatus], parentSchema: StructType, sparkSession: SparkSession): StructType

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
    def read(
                status: FileStatus,
                fs: FileSystem,
                requiredSchema: StructType,
                options: Map[String, String],
                indexSystem: IndexSystem,
                exprConfigOpt: Option[ExprConfig]
    ): Iterator[InternalRow]

}

/** A trait defining the read strategy for the GDAL file format. */
object ReadStrategy {

    /**
      * Returns the read strategy.
      * @param options
      *   Options passed to the reader.
      *
      * @return
      *   Read strategy.
      */
    def getReader(options: Map[String, String]): ReadStrategy = {
        val readStrategy = options.getOrElse(MOSAIC_RASTER_READ_STRATEGY, MOSAIC_RASTER_READ_AS_PATH)

        readStrategy match {
            case MOSAIC_RASTER_READ_IN_MEMORY    => ReadInMemory
            case MOSAIC_RASTER_SUBDIVIDE_ON_READ => SubdivideOnRead
            case MOSAIC_RASTER_READ_AS_PATH      => ReadAsPath
            case "retile_on_read"                => SubdivideOnRead // <- this is for legacy (has been renamed)
            case _                               => ReadAsPath
        }

    }

}
