package com.databricks.labs.mosaic

import com.databricks.labs.mosaic.datasource.UserDefinedReader
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.functions.col
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

    test("multi read on geo_db") {
        val raster = "/binary/geodb/"
        val filePath = getClass.getResource(raster).getPath

        import com.databricks.labs.mosaic

        val df = mosaic.read.format("multi_read_ogr")
            .option("layerNumber", "0")
            .option("vsizip", "true")
            .load(filePath)

        val mc = MosaicContext.build(H3, JTS)
        df.withColumn(
          "wkt", mc.functions.st_astext(col("SHAPE"))
        ).show()
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
