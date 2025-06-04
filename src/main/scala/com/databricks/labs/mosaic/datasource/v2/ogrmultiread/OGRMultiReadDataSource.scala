package com.databricks.labs.mosaic.datasource.v2.ogrmultiread

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.datasource.OGRFileFormat
import com.databricks.labs.mosaic.utils.HadoopUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.TableProvider
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

import java.net.URI
import scala.jdk.CollectionConverters.mapAsScalaMapConverter

class OGRMultiReadDataSource extends TableProvider with DataSourceRegister {

    override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
        val sparkSession = SparkSession.builder.getOrCreate
        GDAL.enable(sparkSession)

        val path = new org.apache.hadoop.fs.Path(new URI(options.get("path")))
        val hConf = new SerializableConfiguration(sparkSession.sessionState.newHadoopConf)
        val fs = path.getFileSystem(hConf.value)
        val files = HadoopUtils.listHadoopFiles(options.get("path"), hConf)

        val driverName = if (options.containsKey("driverName")) options.get("driverName") else ""

        OGRFileFormat.inferSchemaImpl(
            driverName,
            files.head.toString,
            options.asCaseSensitiveMap().asScala.toMap,
            hConf
        ).get

    }

    override def getTable(schema: StructType, partitions: Array[Transform], properties: java.util.Map[String, String]): Table = {
        new OGRMultiReadTable(schema, properties.asScala.toMap)
    }

    override def shortName(): String = "ogr_multiread"

}
