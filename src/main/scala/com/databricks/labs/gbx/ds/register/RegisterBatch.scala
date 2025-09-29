package com.databricks.labs.gbx.ds.register

import com.databricks.labs.gbx
import com.databricks.labs.gbx.gridx
import com.databricks.labs.gbx.rasterx.functions
import com.databricks.labs.gbx.vectorx.jts
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType

class RegisterBatch(schema: StructType, options: Map[String, String]) extends Scan with Batch {

    override def readSchema(): StructType = schema

    override def toBatch: Batch = this

    override def planInputPartitions(): Array[InputPartition] = {
        val registerWhat = options.getOrElse("functions", "all")
        registerWhat match {
            case "gridx.bng"      => gridx.bng.functions.register(SparkSession.active)
            case "vectorx.jts.legacy" => jts.legacy.functions.register(SparkSession.active)
            case "rasterx"        => functions.register(SparkSession.active)
            case "all"            =>
                gridx.bng.functions.register(SparkSession.active)
                jts.legacy.functions.register(SparkSession.active)
                gbx.rasterx.functions.register(SparkSession.active)
        }
        Seq.empty[InputPartition].toArray // No data to read, just perform registration
    }

    // No actual reader needed since no data is read
    override def createReaderFactory(): PartitionReaderFactory = (_: InputPartition) => null

}
