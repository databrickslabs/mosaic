package com.databricks.labs.mosaic.expressions.format

import java.util.UUID

import com.databricks.labs.mosaic.core.types.model.{CDMAttribute, CDMDescription, CDMVariable}

import collection.JavaConverters._

import ucar.{ma2, nc2}

import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

case class CDMAttributes(inContent: Expression) extends UnaryExpression with NullIntolerant with CodegenFallback {

    private lazy val encoder = ExpressionEncoder[CDMDescription]()
    override lazy val dataType: DataType = encoder.schema

    override def child: Expression = inContent

    override protected def withNewChildInternal(newChild: Expression): Expression = copy(inContent = newChild)

    override def nullSafeEval(input: Any): Any = {
        val bytes = input.asInstanceOf[Array[Byte]]
        val parser = new CDMParser(bytes)
        parser.description.serialize
    }

}

private class CDMParser(bytes: Array[Byte]) {

    val ncFile: nc2.NetcdfFile = nc2.NetcdfFiles.openInMemory(UUID.randomUUID.toString, bytes)

    val nc2variables: Array[nc2.Variable] = ncFile.getVariables.asScala.toArray
    val variableNames: Array[String] = nc2variables.map(_.getShortName)

    def attributes(v: String): Array[CDMAttribute] =
        ncFile
            .findVariable(v)
            .attributes
            .asScala
            .map(a =>
                (a.getShortName, a.getDataType) match {
                    case (n: String, ma2.DataType.BYTE | ma2.DataType.UBYTE)   => CDMAttribute.fromTypedValue[Byte](n, a.getValue(0))
                    case (n: String, ma2.DataType.STRING | ma2.DataType.CHAR)  => CDMAttribute.fromTypedValue[String](n, a.getValue(0))
                    case (n: String, ma2.DataType.SHORT | ma2.DataType.USHORT) => CDMAttribute.fromTypedValue[Short](n, a.getValue(0))
                    case (n: String, ma2.DataType.INT | ma2.DataType.UINT)     => CDMAttribute.fromTypedValue[Int](n, a.getValue(0))
                    case (n: String, ma2.DataType.LONG | ma2.DataType.ULONG)   => CDMAttribute.fromTypedValue[Long](n, a.getValue(0))
                    case (n: String, ma2.DataType.FLOAT)                       => CDMAttribute.fromTypedValue[Float](n, a.getValue(0))
                    case (n: String, ma2.DataType.DOUBLE)                      => CDMAttribute.fromTypedValue[Double](n, a.getValue(0))
                }
            )
            .toArray

    def description: CDMDescription = CDMDescription(nc2variables.map(v => CDMVariable(v.getShortName, attributes(v.getFullName))))

}
