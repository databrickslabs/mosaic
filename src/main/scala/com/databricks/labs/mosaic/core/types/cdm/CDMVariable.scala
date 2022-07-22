package com.databricks.labs.mosaic.core.types.cdm

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.unsafe.types.UTF8String

case class CDMVariable(
    variable: String,
    data3dInt: CDMArray3d[Int],
    data3dDbl: CDMArray3d[Double]
) {
    def serialize: Any = InternalRow.fromSeq(Seq(UTF8String.fromString(variable), data3dInt.serialize))
}
