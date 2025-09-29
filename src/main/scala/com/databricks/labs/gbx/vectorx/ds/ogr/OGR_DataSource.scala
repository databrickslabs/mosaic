package com.databricks.labs.gbx.vectorx.ds.ogr

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.util.{HadoopUtils, NodeFileManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration
import org.gdal.ogr.ogr

import scala.jdk.CollectionConverters.MapHasAsScala

//noinspection ScalaUnusedSymbol
class OGR_DataSource extends TableProvider with DataSourceRegister {

    override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
        val driverName = if (options.containsKey("driverName")) options.get("driverName") else ""

        val sparkSession = SparkSession.builder.getOrCreate
        val config = ExpressionConfig(sparkSession)
        GDALManager.init(config)
        ogr.RegisterAll()

        val hConf = new SerializableConfiguration(sparkSession.sessionState.newHadoopConf)
        val headPath = HadoopUtils.getFirstFile(options.get("path"), hConf)

        NodeFileManager.init(hConf)
        val localPath = NodeFileManager.readRemote(headPath)

        val schema = OGR_SchemaInference
            .inferSchemaImpl(
              driverName,
              localPath,
              options.asCaseSensitiveMap().asScala.toMap
            )
            .get

        NodeFileManager.releaseRemote(headPath)
        schema
    }

    override def getTable(schema: StructType, partitions: Array[Transform], properties: java.util.Map[String, String]): Table = {
        new OGR_Table(schema, properties.asScala.toMap)
    }

    override def shortName(): String = "ogr"

}
