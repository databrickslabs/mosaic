package com.databricks.labs.mosaic.datasource

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.util.SerializableConfiguration

class ShapefileFileFormat extends OGRFileFormat with DataSourceRegister with Serializable {

    private val driverName = "ESRI Shapefile"

    override def shortName(): String = "shapefile"

    private def checkExtension(path: String): Boolean = {
        path.endsWith(".shp") || path.endsWith(".zip")
    }

    override def inferSchema(
        sparkSession: SparkSession,
        options: Map[String, String],
        files: Seq[FileStatus]
    ): Option[StructType] = {
        val headFilePath = files.map(_.getPath.toString).filter(checkExtension).head
        val hConf = new SerializableConfiguration(sparkSession.sessionState.newHadoopConf)
        OGRFileFormat.inferSchemaImpl(driverName, headFilePath, options, hConf)
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
        val hConf = new SerializableConfiguration(hadoopConf)
        (file: PartitionedFile) => {
            if (checkExtension(file.filePath.toString())) {
                OGRFileFormat.buildReaderImpl(driverName, dataSchema, requiredSchema, options, hConf)(file)
            } else {
                Seq.empty[InternalRow].iterator
            }
        }
    }

}
