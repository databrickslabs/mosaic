package com.databricks.labs.mosaic.datasource.v2.ogrmultiread

import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

case class OGRMultiReadPartition(
    filePath: String,
    driver: String,
    layer: String,
    asWKB: Boolean,
    schema: StructType,
    start: Int,
    end: Int,
    hConf: SerializableConfiguration
) extends InputPartition
      with Serializable
