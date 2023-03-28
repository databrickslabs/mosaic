package com.databricks.labs.mosaic.datasource

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.matchers.must.Matchers.{be, noException}

class UserDefinedReaderTest extends QueryTest with SharedSparkSession {

    test("User defined reader") {

        val raster = "/binary/netcdf-coral/"
        val filePath = getClass.getResource(raster).getPath

        val df = spark.read
            .format("user_defined_reader")
            .option("readerClass", "com.databricks.labs.mosaic.datasource.TestUDReader")
            .load(filePath)

        noException should be thrownBy df.collect()
    }

}

class TestUDReader extends UserDefinedReader {

    override def inferSchema(sparkSession: SparkSession, options: Map[String, String], files: Seq[FileStatus]): Option[StructType] = {
        Some(StructType(Seq(StructField("test", StringType, nullable = false))))
    }

    override def buildReader(
        sparkSession: SparkSession,
        dataSchema: StructType,
        partitionSchema: StructType,
        requiredSchema: StructType,
        filters: Seq[Filter],
        options: Map[String, String],
        hadoopConf: Configuration
    ): PartitionedFile => Iterator[InternalRow] = { file => Iterator(InternalRow(UTF8String.fromString("Hello"))) }

}
