package com.databricks.labs.mosaic.datasource

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.sources.Filter

class OpenGeoDBFileFormat extends OGRFileFormat {

    private val driverName = "OpenFileGDB"

    override def shortName(): String = "open_geo_db"

    override def inferSchema(
        sparkSession: SparkSession,
        options: Map[String, String],
        files: Seq[FileStatus]
    ): Option[StructType] = {
        val layerN = options.getOrElse("layerNumber", "0").toInt
        val inferenceLimit = options.getOrElse("inferenceLimit", "200").toInt
        val useZipPath = options.getOrElse("vsizip", "false").toBoolean
        OGRFileFormat.inferSchemaImpl(driverName, layerN, inferenceLimit, useZipPath, files)
    }

    override def buildReader(
        sparkSession: SparkSession,
        dataSchema: StructType,
        partitionSchema: StructType,
        requiredSchema: StructType,
        filters: Seq[Filter],
        options: Map[String, String],
        hadoopConf: Configuration
    ): PartitionedFile => Iterator[InternalRow] = {
        val layerN = options.getOrElse("layerNumber", "0").toInt
        val useZipPath = options.getOrElse("vsizip", "false").toBoolean
        OGRFileFormat.buildReaderImpl(driverName, layerN, useZipPath, dataSchema)
    }

}
