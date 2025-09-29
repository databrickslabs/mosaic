package com.databricks.labs.gbx.vectorx.ds.ogr

import com.databricks.labs.gbx.expressions.ExpressionConfig
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.types.StructType

case class OGR_Partition(
    filePath: String,
    driver: String,
    layer: String,
    asWKB: Boolean,
    schema: StructType,
    start: Int,
    end: Int,
    expressionConfig: ExpressionConfig
) extends InputPartition
      with Serializable
