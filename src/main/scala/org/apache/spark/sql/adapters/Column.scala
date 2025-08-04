package org.apache.spark.sql.adapters

import org.apache.spark.sql.{Column => SparkColumn}
import org.apache.spark.sql.catalyst.expressions.Expression

object Column {

    def apply(fnName: String, args: Seq[SparkColumn]): SparkColumn = { SparkColumn.fn(fnName, args: _*) }

    def apply(expr: Expression): SparkColumn = {
        null //new SparkColumn(expr)
    }

}
