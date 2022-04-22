package com.databricks.labs.mosaic.core.types.cdm

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String

case class CDMAttribute(
    attributeName: String,
    stringValue: String,
    originalValueDType: String
) {
    def serialize: InternalRow =
        InternalRow.fromSeq(
          Seq(
            UTF8String.fromString(attributeName),
            UTF8String.fromString(stringValue),
            UTF8String.fromString(originalValueDType)
          )
        )
}

object CDMAttribute {
    def fromTypedValue[T](
        attributeName: String,
        value: Any
    ): CDMAttribute = {
        value.asInstanceOf[T] match {
            case b: Byte   => CDMAttribute(attributeName, s"$b", "byte") // CDMAttribute(attributeName, 1, b, "", 0, 0, 0L, 0f, 0d)
            case s: String => CDMAttribute(attributeName, s"$s", "string") // CDMAttribute(attributeName, 2, 0x0, s, 0, 0, 0L, 0f, 0d)
            case sh: Short => CDMAttribute(attributeName, s"$sh", "short") // CDMAttribute(attributeName, 3, 0x0, "", sh, 0, 0L, 0f, 0d)
            case i: Int    => CDMAttribute(attributeName, s"$i", "int") // CDMAttribute(attributeName, 4, 0x0, "", 0, i, 0L, 0f, 0d)
            case l: Long   => CDMAttribute(attributeName, s"$l", "long") // CDMAttribute(attributeName, 5, 0x0, "", 0, 0, l, 0f, 0d)
            case f: Float  => CDMAttribute(attributeName, s"$f", "float") // CDMAttribute(attributeName, 6, 0x0, "", 0, 0, 0L, f, 0d)
            case d: Double => CDMAttribute(attributeName, s"$d", "double") // CDMAttribute(attributeName, 7, 0x0, "", 0, 0, 0L, 0f, d)

        }
    }
}
