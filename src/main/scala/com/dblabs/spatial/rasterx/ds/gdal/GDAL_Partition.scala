package com.dblabs.spatial.rasterx.ds.gdal

import com.dblabs.spatial.expressions.ExpressionConfig
import org.apache.spark.sql.connector.read.InputPartition

case class GDAL_Partition(
    filePath: String,
    sizeInMB: Int,
    expressionConfig: ExpressionConfig
) extends InputPartition
      with Serializable
