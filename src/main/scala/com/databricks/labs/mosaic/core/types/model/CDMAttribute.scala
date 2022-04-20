package com.databricks.labs.mosaic.core.types.model

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String

case class CDMAttribute(
    attributeName: String,
    valIndex: Int,
    byteValue: Byte,
    strValue: String,
    shortValue: Short,
    intValue: Int,
    longValue: Long,
    fltValue: Float,
    dblValue: Double
) {
    def serialize: InternalRow =
        InternalRow.fromSeq(
          Seq(
            UTF8String.fromString(attributeName),
            valIndex,
            byteValue,
            UTF8String.fromString(strValue),
            shortValue,
            intValue,
            longValue,
            fltValue,
            dblValue
          )
        )
}
object CDMAttribute {
    def fromTypedValue[T](
        attributeName: String,
        value: Any
    ): CDMAttribute = {
        value.asInstanceOf[T] match {
            case b: Byte   => CDMAttribute(attributeName, 1, b, "", 0, 0, 0L, 0f, 0d)
            case s: String => CDMAttribute(attributeName, 2, 0x0, s, 0, 0, 0L, 0f, 0d)
            case sh: Short => CDMAttribute(attributeName, 3, 0x0, "", sh, 0, 0L, 0f, 0d)
            case i: Int    => CDMAttribute(attributeName, 4, 0x0, "", 0, i, 0L, 0f, 0d)
            case l: Long   => CDMAttribute(attributeName, 5, 0x0, "", 0, 0, l, 0f, 0d)
            case f: Float  => CDMAttribute(attributeName, 6, 0x0, "", 0, 0, 0L, f, 0d)
            case d: Double => CDMAttribute(attributeName, 7, 0x0, "", 0, 0, 0L, 0f, d)

        }
    }
}
