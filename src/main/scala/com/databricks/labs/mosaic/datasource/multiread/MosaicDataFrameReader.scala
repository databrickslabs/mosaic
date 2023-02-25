package com.databricks.labs.mosaic.datasource.multiread

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

class MosaicDataFrameReader(sparkSession: SparkSession) {

    protected var extraOptions: CaseInsensitiveMap[String] = CaseInsensitiveMap[String](Map.empty)

    protected var format: String = ""

    def option(key: String, value: String): MosaicDataFrameReader = {
        extraOptions += (key -> value)
        this
    }

    def format(source: String): MosaicDataFrameReader = {
        this.format = source
        this
    }

    def load(path: String): DataFrame = {
        format match {
            case "multi_read_ogr" => new OGRMultiReadDataFrameReader(sparkSession).load(path)
            case _                => throw new Error(s"Unsupported format: $format")
        }
    }

    def load(paths: String*): DataFrame = {
        format match {
            case "multi_read_ogr" => new OGRMultiReadDataFrameReader(sparkSession).load(paths: _*)
            case _                => throw new Error(s"Unsupported format: $format")
        }
    }

}
