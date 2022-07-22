package com.databricks.labs.mosaic.core.types.cdm

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.unsafe.types.UTF8String

case class CDMDataset(
    variables: Array[CDMVariable]
) {
    def serialize: Any = InternalRow.fromSeq(Seq(ArrayData.toArrayData(variables.map(_.serialize))))
}
