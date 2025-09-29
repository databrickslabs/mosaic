package com.databricks.labs.gbx.ds.whitelist

import com.databricks.labs.gbx.util.HadoopUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import scala.util.Try

class WhitelistBatch(schema: StructType, options: Map[String, String]) extends Scan with Batch {

    override def readSchema(): StructType = schema

    override def toBatch: Batch = this

    override def planInputPartitions(): Array[InputPartition] = {
        val rootPath = options.getOrElse("path", throw new IllegalArgumentException("Option 'path' is required for Whitelist data source"))
        val hconf = new SerializableConfiguration(SparkSession.builder().getOrCreate().sessionState.newHadoopConf())
        Try(HadoopUtils.listAllHadoopFiles(rootPath, hconf, "", dropEmpty = true))
        Array(new WhitelistPartition)
    }

    override def createReaderFactory(): PartitionReaderFactory = (_: InputPartition) => new WhitelistReader()

}

