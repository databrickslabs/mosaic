package com.databricks.labs.gbx.rasterx.ds.gdal

import com.databricks.labs.gbx.expressions.ExpressionConfig
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.connector.write.{BatchWrite, Write, WriteBuilder}
import org.apache.spark.sql.types.StructType

class GDAL_WriteBuilder(schema: StructType, options: Map[String, String]) extends WriteBuilder {
    override def build(): Write = {
        val spark = SparkSession.builder().getOrCreate()
        val ec = ExpressionConfig(spark)
        new Write {
            override def toBatch: BatchWrite = new GDAL_BatchWrite(schema, options, ec)
        }
    }
}
