package com.databricks.labs.gbx.rasterx.ds.gdal

import com.databricks.labs.gbx.rasterx.util.RST_ExpressionUtil
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{BinaryType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters.MapHasAsScala

//noinspection ScalaUnusedSymbol
class GDAL_DataSource extends TableProvider with DataSourceRegister {

    override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
        StructType(
          Array(
            StructField("source", StringType),
            StructField("tile", RST_ExpressionUtil.tileDataType(BinaryType))
          )
        )
    }

    override def getTable(schema: StructType, partitions: Array[Transform], properties: java.util.Map[String, String]): Table = {
        new GDAL_Table(schema, properties.asScala.toMap)
    }

    override def shortName(): String = "gdal"

}
