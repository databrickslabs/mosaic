package com.databricks.labs.gbx.ds

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters._

trait DataSourceExtras {

    def dsExtraMap(): Map[String, String]

    def extraJavaUtilMap(properties: java.util.Map[String, String]): java.util.Map[String, String] = {
        val newProperties = properties.asScala.toMap ++ dsExtraMap()
        newProperties.asJava
    }

    def extraCaseInsensitiveStringMap(options: CaseInsensitiveStringMap): CaseInsensitiveStringMap = {
        val newMap = options.asCaseSensitiveMap().asScala.toMap ++ dsExtraMap()
        new CaseInsensitiveStringMap(newMap.asJava)
    }

}
