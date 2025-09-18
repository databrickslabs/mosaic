package com.dblabs.spatial.vectorx.jts.legacy

import com.dblabs.spatial.expressions.RegistryDelegate
import com.dblabs.spatial.vectorx.jts.legacy.expressions.ST_LegacyAsWKB
import org.apache.spark.sql.adapters.{Column => ColumnAdapter}
import org.apache.spark.sql.{Column, SparkSession}

object functions extends Serializable {

    val flag = "com.dblabs.spatial.vectorx.jts.legacy.registered"

    def register(spark: SparkSession): Unit = {
        val sc = spark.sparkContext
        if (sc.getConf.get(flag, "false") == "true") return // Prevent multiple registrations

        val registry = spark.sessionState.functionRegistry
        val rd = RegistryDelegate(registry)
        rd.register(ST_LegacyAsWKB)

        sc.getConf.set(flag, "true")
    }

    def st_legacyaswkb(geom: Column): Column = ColumnAdapter("st_legacyaswkb", Seq(geom))

}
