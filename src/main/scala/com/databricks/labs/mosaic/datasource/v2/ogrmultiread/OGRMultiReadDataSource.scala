package com.databricks.labs.mosaic.datasource.v2.ogrmultiread

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.datasource.OGRFileFormat
import com.databricks.labs.mosaic.utils.HadoopUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

import scala.jdk.CollectionConverters.mapAsScalaMapConverter

//noinspection ScalaUnusedSymbol
class OGRMultiReadDataSource extends TableProvider with DataSourceRegister {

    override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
        val sparkSession = SparkSession.builder.getOrCreate
        GDAL.enable(sparkSession)

        val hConf = new SerializableConfiguration(sparkSession.sessionState.newHadoopConf)
        val files = HadoopUtils.listHadoopFiles(options.get("path"), hConf)

        val driverName = if (options.containsKey("driverName")) options.get("driverName") else ""

        OGRFileFormat
            .inferSchemaImpl(
              driverName,
              files.head,
              options.asCaseSensitiveMap().asScala.toMap,
              hConf
            )
            .get

    }

    override def getTable(schema: StructType, partitions: Array[Transform], properties: java.util.Map[String, String]): Table = {
        new OGRMultiReadTable(schema, properties.asScala.toMap)
    }

    override def shortName(): String = "ogr_multiread"

}
