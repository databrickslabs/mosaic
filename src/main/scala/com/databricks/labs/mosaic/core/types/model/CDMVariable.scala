//package com.databricks.labs.mosaic.core.types.model
//
//import org.apache.spark.sql.catalyst.InternalRow
//import org.apache.spark.sql.catalyst.util.ArrayData
//import org.apache.spark.unsafe.types.UTF8String
//
//case class CDMVariable(
//    variable: String,
//    data: CDMArray
//) {
//    def serialize: Any = InternalRow.fromSeq(Seq(UTF8String.fromString(variable), ArrayData.toArrayData(data.serialize)))
//}
