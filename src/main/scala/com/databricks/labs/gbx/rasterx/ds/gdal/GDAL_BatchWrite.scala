package com.databricks.labs.gbx.rasterx.ds.gdal

import com.databricks.labs.gbx.expressions.ExpressionConfig
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

class GDAL_BatchWrite(schema: StructType, options: Map[String, String], expressionConfig: ExpressionConfig) extends BatchWrite {

    override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
        val root = options("path")
        val nameCol = options.get("nameCol")
        val ext = options.getOrElse("ext", "tif") // default to tif
        new GDAL_DataWriterFactory(schema, root, nameCol, ext, expressionConfig)
    }
    override def commit(messages: Array[WriterCommitMessage]): Unit = ()
    override def abort(messages: Array[WriterCommitMessage]): Unit = ()

}
