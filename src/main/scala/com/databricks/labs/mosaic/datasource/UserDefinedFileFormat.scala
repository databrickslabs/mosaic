package com.databricks.labs.mosaic.datasource

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.StructType

/**
  * A base Spark SQL data source for reading any file format using User Defined
  * Code. To be used for file format injection at script level.
  */
//noinspection ScalaStyle
class UserDefinedFileFormat extends FileFormat with DataSourceRegister with Serializable {

    override def inferSchema(
        sparkSession: SparkSession,
        options: Map[String, String],
        files: Seq[FileStatus]
    ): Option[StructType] = {
        val readerClass = options("readerClass")
        val reader = Class.forName(readerClass).newInstance().asInstanceOf[UserDefinedReader]
        reader.inferSchema(sparkSession, options, files)
    }

    override def isSplitable(sparkSession: SparkSession, options: Map[String, String], path: Path): Boolean = {
        true
    }

    override def prepareWrite(
        sparkSession: SparkSession,
        job: Job,
        options: Map[String, String],
        dataSchema: StructType
    ): OutputWriterFactory = throw new UnsupportedOperationException("Write is not supported for this data source")

    override def shortName(): String = "user_defined_reader"

    override def buildReader(
        sparkSession: SparkSession,
        dataSchema: StructType,
        partitionSchema: StructType,
        requiredSchema: StructType,
        filters: Seq[Filter],
        options: Map[String, String],
        hadoopConf: Configuration
    ): PartitionedFile => Iterator[InternalRow] = {
        val readerClass = options("readerClass")
        val reader = Class.forName(readerClass).newInstance().asInstanceOf[UserDefinedReader]
        reader.buildReader(sparkSession, dataSchema, partitionSchema, requiredSchema, filters, options, hadoopConf)
    }

}
