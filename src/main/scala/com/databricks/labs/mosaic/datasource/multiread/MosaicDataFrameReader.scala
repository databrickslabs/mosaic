package com.databricks.labs.mosaic.datasource.multiread

import org.apache.spark.sql.{DataFrame, SparkSession}

class MosaicDataFrameReader(sparkSession: SparkSession) {

    protected var extraOptions: Map[String, String] = Map.empty[String, String]

    protected var format: String = ""

    def option(key: String, value: String): MosaicDataFrameReader = {
        extraOptions += (key -> value)
        this
    }

    def options(map: Map[String, String]): MosaicDataFrameReader = {
        extraOptions = map
        this
    }

    def format(source: String): MosaicDataFrameReader = {
        this.format = source
        this
    }

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
