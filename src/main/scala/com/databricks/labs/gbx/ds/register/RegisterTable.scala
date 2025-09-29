package com.databricks.labs.gbx.ds.register

import org.apache.spark.sql.connector.catalog.{Column, SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters.{MapHasAsScala, SetHasAsJava}

class RegisterTable(schema: StructType, properties: Map[String, String]) extends Table with SupportsRead {

    override def name(): String = "register_ds"

    // noinspection ScalaDeprecation
    override def schema(): StructType = schema

    override def columns(): Array[Column] = schema.fields.map(f => Column.create(f.name, f.dataType, f.nullable))

    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = { () =>
        new RegisterBatch(schema, properties ++ options.asScala)
    }

    override def capabilities(): java.util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava

}
