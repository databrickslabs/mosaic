package com.databricks.labs.gbx.rasterx.ds.gdal

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.util.{HadoopUtils, NodeFileManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType

class GDAL_Batch(schema: StructType, options: Map[String, String]) extends Scan with Batch {

    override def readSchema(): StructType = schema

    override def toBatch: Batch = this

    override def planInputPartitions(): Array[InputPartition] = {
        val inPath = options("path")
        val sizeInMB = options.getOrElse("sizeInMB", "16").toInt
        val filterRegex = options.getOrElse("filterRegex", ".*")

        val sparkSession = SparkSession.builder.getOrCreate
        val exprConfig = ExpressionConfig(sparkSession)
        NodeFileManager.init(exprConfig.hConf)

        val files = HadoopUtils.listAllHadoopFiles(inPath, exprConfig.hConf, filterRegex)
        val partitions = files.map { file => GDAL_Partition(file, sizeInMB, exprConfig) }
        partitions.toArray
    }

    override def createReaderFactory(): PartitionReaderFactory =
        (partition: InputPartition) => {
            new GDAL_Reader(partition.asInstanceOf[GDAL_Partition])
        }



}
