package com.databricks.labs.mosaic.expressions.format

import java.util.Locale

import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

case class GetCDMContent(binaryFileContent: Expression, variable: String, rank: Int, dType: String)
    extends UnaryExpression
      with NullIntolerant
      with CodegenFallback {

    // TODO how to infer?
    private lazy val encoder =
        (rank, dType.toLowerCase(Locale.ROOT)) match {
            case (1, "int" | "uint")     => ArrayType(IntegerType)
            case (1, "short" | "ushort") => ArrayType(ShortType)
            case (1, "long")             => ArrayType(LongType)
            case (1, "float")            => ArrayType(FloatType)
            case (1, "double")           => ArrayType(DoubleType)
            case (1, "byte" | "ubyte")   => ArrayType(ByteType)
            case (1, "string" | "char")  => ArrayType(StringType)
            case (2, "int" | "uint")     => ArrayType(ArrayType(IntegerType))
            case (2, "short" | "ushort") => ArrayType(ArrayType(ShortType))
            case (2, "long")             => ArrayType(ArrayType(LongType))
            case (2, "float")            => ArrayType(ArrayType(FloatType))
            case (2, "double")           => ArrayType(ArrayType(DoubleType))
            case (2, "byte" | "ubyte")   => ArrayType(ArrayType(ByteType))
            case (2, "string" | "char")  => ArrayType(ArrayType(StringType))
            case (3, "int" | "uint")     => ArrayType(ArrayType(ArrayType(IntegerType)))
            case (3, "short" | "ushort") => ArrayType(ArrayType(ArrayType(ShortType)))
            case (3, "long")             => ArrayType(ArrayType(ArrayType(LongType)))
            case (3, "float")            => ArrayType(ArrayType(ArrayType(FloatType)))
            case (3, "double")           => ArrayType(ArrayType(ArrayType(DoubleType)))
            case (3, "byte" | "ubyte")   => ArrayType(ArrayType(ArrayType(ByteType)))
            case (3, "string" | "char")  => ArrayType(ArrayType(ArrayType(StringType)))
        }

    override lazy val dataType: DataType = encoder

    override def child: Expression = binaryFileContent

    override protected def withNewChildInternal(newChild: Expression): Expression = copy(binaryFileContent = newChild)

    override def nullSafeEval(input: Any): Any = {
        val bytes = input.asInstanceOf[Array[Byte]]
        val parser = new CDMParser(bytes)
        val data = parser.read[Byte](variable)
        data.serialize
    }

}
