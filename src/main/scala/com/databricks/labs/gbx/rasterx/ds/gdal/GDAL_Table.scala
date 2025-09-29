package com.databricks.labs.gbx.rasterx.ds.gdal

import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters._

class GDAL_Table(schema: StructType, properties: Map[String, String]) extends Table with SupportsRead with SupportsWrite {

    override def name(): String = "gdal"

    // noinspection ScalaDeprecation
    override def schema(): StructType = schema

    override def columns(): Array[Column] = schema.fields.map(f => Column.create(f.name, f.dataType, f.nullable))

    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = { () =>
        new GDAL_Batch(schema, properties ++ options.asScala)
    }

    override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
        new GDAL_WriteBuilder(info.schema(), properties ++ info.options().asScala)
    }

    override def capabilities(): java.util.Set[TableCapability] = Set(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE).asJava

}
