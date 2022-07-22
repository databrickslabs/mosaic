package com.databricks.labs.mosaic.core.types.cdm

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.unsafe.types.UTF8String

case class CDMVariableAttributes(
    variable: String,
    rank: Int,
    dType: String,
    attributes: Array[CDMAttribute]
) {
    def serialize: Any =
        InternalRow.fromSeq(
          Seq(UTF8String.fromString(variable), rank, UTF8String.fromString(dType), ArrayData.toArrayData(attributes.map(_.serialize)))
        )
}
