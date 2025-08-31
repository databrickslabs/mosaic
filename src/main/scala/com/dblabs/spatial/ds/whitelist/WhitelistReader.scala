package com.dblabs.spatial.ds.whitelist

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.PartitionReader

class WhitelistReader extends PartitionReader[InternalRow] {

    override def next(): Boolean = false

    override def get(): InternalRow = {
        val row = new GenericInternalRow(1)
        row.setNullAt(0)
        row
    }

    override def close(): Unit = {
        // No resources to close
    }

}
