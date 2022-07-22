package com.databricks.labs.mosaic.core.types.cdm

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData

case class CDMStructure(
    variables: Array[CDMVariableAttributes]
) {
    def serialize: Any = InternalRow.fromSeq(Seq(ArrayData.toArrayData(variables.map(_.serialize))))
}
