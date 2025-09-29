package com.databricks.labs.gbx.rasterx.ds.gdal

import com.databricks.labs.gbx.expressions.ExpressionConfig
import org.apache.spark.sql.connector.read.InputPartition

case class GDAL_Partition(
    filePath: String,
    sizeInMB: Int,
    expressionConfig: ExpressionConfig
) extends InputPartition
      with Serializable
