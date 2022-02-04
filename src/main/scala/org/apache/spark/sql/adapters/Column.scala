package org.apache.spark.sql.adapters

import org.apache.spark.sql.{Column => SparkColumn}
import org.apache.spark.sql.catalyst.expressions.Expression

object Column {
    def apply(expr: Expression): SparkColumn = {
        new SparkColumn(expr)
    }
}
