package org.apache.spark.sql.adapters

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.{Column => SparkColumn}

object Column {
  def apply(expr: Expression): SparkColumn = {
    new SparkColumn(expr)
  }
}
