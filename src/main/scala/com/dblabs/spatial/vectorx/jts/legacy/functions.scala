package com.dblabs.spatial.vectorx.jts.legacy

import com.dblabs.spatial.expressions.RegistryDelegate
import com.dblabs.spatial.vectorx.jts.legacy.expressions.ST_LegacyAsWKB
import org.apache.spark.sql.adapters.{Column => ColumnAdapter}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, SparkSession}

object functions extends Serializable {

    def register(spark: SparkSession): Unit = {
        val registry = spark.sessionState.functionRegistry
        val rd = RegistryDelegate(registry)
        rd.register(ST_LegacyAsWKB)
    }

    def st_legacyaswkb(geom: Column): Column = ColumnAdapter("st_legacyaswkb", Seq(geom))

}
