package com.databricks.labs.gbx.rasterx.ds.gdal

import org.apache.spark.sql.connector.write.WriterCommitMessage

final case class GDAL_WriterMsg(gdalErrors: Array[String]) extends WriterCommitMessage

