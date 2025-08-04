package com.databricks.labs.mosaic.datasource.v2.ogrmultiread

import org.apache.spark.sql.connector.catalog.{Column, SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters._

class OGRMultiReadTable(schema: StructType, properties: Map[String, String]) extends Table with SupportsRead {

    override def name(): String = "OGRMultiRead"

    // noinspection ScalaDeprecation
    override def schema(): StructType = schema

    override def columns(): Array[Column] = schema.fields.map(f => Column.create(f.name, f.dataType, f.nullable))

    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = { () =>
        new OGRMultiReadBatch(schema, properties ++ options.asScala)
    }

    override def capabilities(): java.util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava

}
