package com.databricks.labs.mosaic.datasource

import com.databricks.labs.mosaic.datasource.OGRFileFormat.{buildReaderImpl, inferSchemaImpl}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.sources.Filter

class GeoDBFileFormat extends OGRFileFormat {

    override def shortName(): String = "geo_db"

    private val driverName = "FileGDB"

    override def inferSchema(
        sparkSession: SparkSession,
        options: Map[String, String],
        files: Seq[FileStatus]
    ): Option[StructType] = {
        val layerN = options.getOrElse("layerNumber", "0").toInt
        inferSchemaImpl(driverName, layerN, files)
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
        buildReaderImpl(driverName, layerN)
    }

}
