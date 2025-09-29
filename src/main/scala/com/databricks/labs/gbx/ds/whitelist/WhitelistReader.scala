package com.databricks.labs.gbx.ds.whitelist

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.PartitionReader

class WhitelistReader extends PartitionReader[InternalRow] {

    private var hasNext = true

    override def next(): Boolean = hasNext

    override def get(): InternalRow = {
        hasNext = false
        val row = new GenericInternalRow(1)
        row.setBoolean(0, value = true)
        row
    }

    override def close(): Unit = {
        // No resources to close
    }

}
