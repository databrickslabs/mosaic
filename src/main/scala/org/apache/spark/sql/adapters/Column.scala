package org.apache.spark.sql.adapters

import org.apache.spark.sql.{Column => SparkColumn}

object Column {

    def apply(fnName: String, args: Seq[SparkColumn]): SparkColumn = { SparkColumn.fn(fnName, args: _*) }

}
