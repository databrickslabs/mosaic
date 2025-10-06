package com.databricks.labs.gbx.vectorx.ds.ogr

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.util.{HadoopUtils, NodeFileManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.functions.{col, explode, udf}
import org.apache.spark.sql.types.StructType

class OGR_Batch(schema: StructType, options: Map[String, String]) extends Scan with Batch {

    override def readSchema(): StructType = schema

    override def toBatch: Batch = this

    override def planInputPartitions(): Array[InputPartition] = {
        val inPath = options("path")
        val chunkSize = options.getOrElse("chunkSize", "10000").toInt
        val driverName = options.getOrElse("driverName", "")
        val layerN = options.getOrElse("layerNumber", "0").toInt
        val layerName = options.getOrElse("layerName", "")
        val asWKB = options.getOrElse("asWKB", "true").toBoolean
        val sparkSession = SparkSession.builder.getOrCreate
        val exprConfig = ExpressionConfig(sparkSession)
        import sparkSession.implicits._

        // This is needed to make sure correct hconf is set for HadoopUtils
        sparkSession.read.format("com.databricks.labs.gbx.ds.whitelist.WhitelistDataSource").option("path", inPath).load()

        NodeFileManager.init(exprConfig.hConf)
        val files = HadoopUtils.listHadoopFiles(inPath, exprConfig.hConf)

        val filesDf = files.toDF("path")

        val offsetsUDF = udf { (path: String) =>
            try {
                // sidecar files will be ignored here
                val localPath = NodeFileManager.readRemote(path)
                val dataset = OGR_Driver.open(localPath, driverName)
                val resolvedLayerName = if (layerName.isEmpty) dataset.GetLayer(layerN).GetName() else layerName
                val layer = dataset.GetLayerByName(resolvedLayerName)
                layer.ResetReading()
                val nRecords = layer.GetFeatureCount().toInt
                NodeFileManager.releaseRemote(path)
                (0 to nRecords by chunkSize).map(s => (path, s, Math.min(s + chunkSize, nRecords))).toArray
            } catch {
                case e: Exception => Array.empty[(String, Int, Int)]
            }

        }

        val offsets = filesDf
            .select(offsetsUDF(filesDf("path")).as("offsets"))
            .select(explode(col("offsets")).as("offset"))
            .select("offset.*")
            .as[(String, Int, Int)]
            .collect()

        val partitions = offsets.map { case (file, start, end) =>
            OGR_Partition(
              file,
              driverName,
              layerName,
              asWKB,
              schema,
              start,
              end,
              exprConfig
            )
        }
        partitions.toArray
    }

    override def createReaderFactory(): PartitionReaderFactory =
        (partition: InputPartition) => {
            new OGR_Reader(partition.asInstanceOf[OGR_Partition])
        }

}
