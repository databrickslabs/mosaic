package com.dblabs.gbx.ds.register

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType
import com.dblabs.gbx.gridx.bng
import com.dblabs.gbx.vectorx.jts.legacy
import com.dblabs.gbx.rasterx

class RegisterBatch(schema: StructType, options: Map[String, String]) extends Scan with Batch {

    override def readSchema(): StructType = schema

    override def toBatch: Batch = this

    override def planInputPartitions(): Array[InputPartition] = {
        val registerWhat = options.getOrElse("functions", "all")
        registerWhat match {
            case "gridx.bng"      => bng.functions.register(SparkSession.active)
            case "vectorx.jts.legacy" => legacy.functions.register(SparkSession.active)
            case "rasterx"        => rasterx.functions.register(SparkSession.active)
            case "all"            =>
                bng.functions.register(SparkSession.active)
                legacy.functions.register(SparkSession.active)
                rasterx.functions.register(SparkSession.active)
        }
        Seq.empty[InputPartition].toArray // No data to read, just perform registration
    }

    // No actual reader needed since no data is read
    override def createReaderFactory(): PartitionReaderFactory = (_: InputPartition) => null

}
