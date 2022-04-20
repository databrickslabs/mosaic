package com.databricks.labs.mosaic.core.types.model

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData

case class CDMDescription(
    variables: Array[CDMVariable]
) {
    def serialize: Any = InternalRow.fromSeq(Seq(ArrayData.toArrayData(variables.map(_.serialize))))
}
