package com.databricks.labs.gbx.ds.register

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{BooleanType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters.MapHasAsScala

class RegisterDataSource extends TableProvider with DataSourceRegister {

    override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
        StructType(Array(StructField("did_read", BooleanType)))
    }

    override def getTable(schema: StructType, partitions: Array[Transform], properties: java.util.Map[String, String]): Table = {
        new RegisterTable(schema, properties.asScala.toMap)
    }

    override def shortName(): String = "register_ds"

}
