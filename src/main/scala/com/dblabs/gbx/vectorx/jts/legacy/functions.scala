package com.dblabs.gbx.vectorx.jts.legacy

import com.dblabs.gbx.expressions.RegistryDelegate
import com.dblabs.gbx.vectorx.jts.legacy.expressions.ST_LegacyAsWKB
import org.apache.spark.sql.adapters.{Column => ColumnAdapter}
import org.apache.spark.sql.{Column, SparkSession}

object functions extends Serializable {

    val flag = "com.dblabs.gbx.vectorx.jts.legacy.registered"

    def register(spark: SparkSession): Unit = {
        val sc = spark.sparkContext
        if (sc.getConf.get(flag, "false") == "true") return // Prevent multiple registrations

        val registry = spark.sessionState.functionRegistry
        val rd = RegistryDelegate(registry)
        rd.register(ST_LegacyAsWKB)

        sc.getConf.set(flag, "true")
    }

    def st_legacyaswkb(geom: Column): Column = ColumnAdapter(ST_LegacyAsWKB.name, Seq(geom))

}
