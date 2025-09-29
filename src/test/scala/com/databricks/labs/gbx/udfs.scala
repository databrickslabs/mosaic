package com.databricks.labs.gbx

import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.util.Try

object udfs {

    def st_aswkb: UserDefinedFunction =
        udf((wkt: String) => {
            JTS.toWKB(JTS.fromWKT(wkt))
        })

    def st_aswkt: UserDefinedFunction =
        udf((wkb: Array[Byte]) => {
            JTS.toWKT(JTS.fromWKB(wkb))
        })

    def st_buffer: UserDefinedFunction =
        udf((wkb: Array[Byte], distance: Double) => {
            JTS.toWKB(JTS.fromWKB(wkb).buffer(distance))
        })

    def st_area: UserDefinedFunction =
        udf((wkb: Array[Byte]) => {
            Try(JTS.fromWKB(wkb).getArea).getOrElse(0.0)
        })

    def st_type: UserDefinedFunction = {
        udf((wkb: Array[Byte]) => {
            JTS.fromWKB(wkb).getGeometryType
        })
    }

}
