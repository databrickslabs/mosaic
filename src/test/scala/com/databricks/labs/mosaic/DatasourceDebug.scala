package com.databricks.labs.mosaic

import com.databricks.labs.mosaic.datasource.UserDefinedReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class DatasourceDebug extends QueryTest with SharedSparkSession {

    test("Debug Datasource") {
        val raster = "/binary/netcdf-coral/ct5km_baa-max-7d_v3.1_20220101.nc"
        val filePath = getClass.getResource(raster).getPath

        val df = spark.read
            .format("binaryFile")
            .option("pathGlobFilter", "*.nc")
            .option("recursiveFileLookup", "true")
            .load(filePath)

    }

    test("User defined reader") {
        val raster = "/binary/netcdf-coral/"
        val filePath = getClass.getResource(raster).getPath

        val df = spark.read
            .format("user_defined_reader")
            .option("readerClass", "com.databricks.labs.mosaic.TestUDReader")
            .load(filePath)

        df.show()
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
