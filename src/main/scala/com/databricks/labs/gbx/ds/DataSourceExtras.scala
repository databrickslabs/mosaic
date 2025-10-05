package com.databricks.labs.gbx.ds

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters._

trait DataSourceExtras {

    def dsExtraMap(checkMap: Map[String, String] = Map.empty): Map[String, String]

    def extraJavaUtilMap(properties: java.util.Map[String, String]): java.util.Map[String, String] = {
        val cMap = properties.asScala.toMap
        val newMap = cMap ++ dsExtraMap(checkMap = cMap)
        newMap.asJava
    }

    def extraCaseInsensitiveStringMap(options: CaseInsensitiveStringMap): CaseInsensitiveStringMap = {
        val cMap = options.asCaseSensitiveMap().asScala.toMap
        val newMap = cMap ++ dsExtraMap(checkMap = cMap)
        new CaseInsensitiveStringMap(newMap.asJava)
    }

}
