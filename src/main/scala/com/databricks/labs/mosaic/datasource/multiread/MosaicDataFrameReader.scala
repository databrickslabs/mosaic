package com.databricks.labs.mosaic.datasource.multiread

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * A Mosaic DataFrame Reader that provides a unified interface to read multiple
  * formats. Currently supports:
  *   - OGR Multi Read
  *   - Raster to Grid
  * @param sparkSession
  *   The Spark Session to use for reading. This is required to create the
  *   DataFrame.
  */
class MosaicDataFrameReader(sparkSession: SparkSession) {

    /** Extra options to pass to the underlying reader. */
    protected var extraOptions: Map[String, String] = Map.empty[String, String]

    /** The format to use for reading. */
    protected var format: String = ""

    /**
      * Set a single option.
      * @param key
      *   The key for the option.
      * @param value
      *   The value for the option.
      * @return
      *   This MosaicDataFrameReader.
      */
    def option(key: String, value: String): MosaicDataFrameReader = {
        extraOptions += (key -> value)
        this
    }

    /**
      * Set a map of options. This will overwrite any existing options.
      * @param map
      *   The map of options to set.
      * @return
      *   This MosaicDataFrameReader.
      */
    def options(map: Map[String, String]): MosaicDataFrameReader = {
        extraOptions = map
        this
    }

    /**
      * Set the format to use for reading.
      * @param source
      *   The format to use.
      * @return
      *   This MosaicDataFrameReader.
      */
    def format(source: String): MosaicDataFrameReader = {
        this.format = source
        this
    }

    /**
      * Load a single path.
      * @param path
      *   The path to load.
      * @return
      *   The DataFrame.
      */
    def load(path: String): DataFrame = {
        format match {
            case "multi_read_ogr" => new OGRMultiReadDataFrameReader(sparkSession)
                    .options(extraOptions)
                    .asInstanceOf[OGRMultiReadDataFrameReader]
                    .load(path)
            case "raster_to_grid" => new RasterAsGridReader(sparkSession)
                    .options(extraOptions)
                    .asInstanceOf[RasterAsGridReader]
                    .load(path)
            case _                => throw new Error(s"Unsupported format: $format")
        }
    }

    /**
      * Load multiple paths.
      * @param paths
      *   The paths to load.
      * @return
      *   The DataFrame.
      */
    def load(paths: String*): DataFrame = {
        format match {
            case "multi_read_ogr" => new OGRMultiReadDataFrameReader(sparkSession)
                    .options(extraOptions)
                    .asInstanceOf[OGRMultiReadDataFrameReader]
                    .load(paths: _*)
            case "raster_to_grid" => new RasterAsGridReader(sparkSession)
                    .options(extraOptions)
                    .asInstanceOf[RasterAsGridReader]
                    .load(paths: _*)
            case _                => throw new Error(s"Unsupported format: $format")
        }
    }

}
