package com.databricks.labs.gbx.vectorx.ds.geojson

import com.databricks.labs.gbx.ds.DataSourceExtras
import com.databricks.labs.gbx.vectorx.ds.ogr.OGR_DataSource
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap


//noinspection ScalaUnusedSymbol
class GeoJSON_DataSource extends OGR_DataSource with DataSourceExtras{

    // default to multi = true given common use
    override def dsExtraMap(checkMap: Map[String, String] = Map.empty): Map[String, String] = {
        if (checkMap.getOrElse("multi", "true").toBoolean) {
            Map("driverName" -> "GeoJSONSeq")
        } else {
            Map("driverName" -> "GeoJSON")
        }
    }

    override def shortName(): String = "geojson"

    override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
        super.inferSchema(extraCaseInsensitiveStringMap(options))
    }

    override def getTable(schema: StructType, partitions: Array[Transform], properties: java.util.Map[String, String]): Table = {
        super.getTable(schema, partitions, extraJavaUtilMap(properties))
    }
}
