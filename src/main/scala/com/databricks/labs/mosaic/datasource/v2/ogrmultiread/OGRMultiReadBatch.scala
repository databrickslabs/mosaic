package com.databricks.labs.mosaic.datasource.v2.ogrmultiread

import com.databricks.labs.mosaic.datasource.OGRFileFormat
import com.databricks.labs.mosaic.utils.HadoopUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

class OGRMultiReadBatch(schema: StructType, options: Map[String, String]) extends Scan with Batch {

    override def readSchema(): StructType = schema

    override def toBatch: Batch = this

    override def planInputPartitions(): Array[InputPartition] = {
        val inPath = options("path")
        val chunkSize = options("chunkSize").toInt
        val driverName = options.getOrElse("driverName", "")
        val layerN = options.getOrElse("layerNumber", "0").toInt
        val layerName = options.getOrElse("layerName", "")
        val asWKB = options.getOrElse("asWKB", "true").toBoolean
        val sparkSession = SparkSession.builder.getOrCreate
        val hConf = new SerializableConfiguration(sparkSession.sessionState.newHadoopConf)

        val files = HadoopUtils.listHadoopFiles(inPath, hConf)

        val partitions = files.flatMap(file => {
            val path = HadoopUtils.copyToLocalTmp(file, hConf)
            val dataset = OGRFileFormat.getDataSource(driverName, path)
            val resolvedLayerName = if (layerName.isEmpty) dataset.GetLayer(layerN).GetName() else layerName
            val layer = dataset.GetLayerByName(resolvedLayerName)
            layer.ResetReading()
            val nRecords = layer.GetFeatureCount().toInt
            val res = (0 to nRecords by chunkSize).map(s =>
                OGRMultiReadPartition(
                  file,
                  driverName,
                  resolvedLayerName,
                  asWKB,
                  schema,
                  s,
                  Math.min(s + chunkSize, nRecords),
                  hConf
                )
            )
            HadoopUtils.deleteIfExists(path, hConf)
            res
        })
        partitions.toArray
    }

    override def createReaderFactory(): PartitionReaderFactory =
        (partition: InputPartition) => {
            new OGRMultiReadReader(partition.asInstanceOf[OGRMultiReadPartition])
        }

}
