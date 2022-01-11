package com.databricks.mosaic.sql

import java.util.Locale

import scala.util.Try

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.functions.{col, expr}

object Prettifier {

    def prettified(df: DataFrame): DataFrame = {
        val functionRegistry = SparkSession.builder().getOrCreate().sessionState.functionRegistry
        require(functionRegistry.functionExists(FunctionIdentifier("st_aswkt")), "Mosaic Context has not registered the functions.")

        // add static method

        val keywords = List("WKB_", "_WKB", "_HEX", "HEX_", "COORDS_", "_COORDS", "POLYGON", "POINT", "GEOMETRY")

        val casted = df.columns
            .map(colName =>
                Try {
                    if (keywords.exists(kw => colName.toUpperCase(Locale.ROOT).contains(kw))) {
                        expr(s"st_aswkt($colName)")
                    } else {
                        col(colName)
                    }
                }.getOrElse(col(colName))
            )
            .toSeq

        df.select(casted: _*)
    }

}
