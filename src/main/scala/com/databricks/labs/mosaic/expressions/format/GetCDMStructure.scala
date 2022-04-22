package com.databricks.labs.mosaic.expressions.format

import com.databricks.labs.mosaic.core.types.cdm.CDMStructure

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

case class GetCDMStructure(binaryFileContent: Expression) extends UnaryExpression with NullIntolerant with CodegenFallback {

    private lazy val encoder = ExpressionEncoder[CDMStructure]()
    override lazy val dataType: DataType = encoder.schema

    override def child: Expression = binaryFileContent

    override protected def withNewChildInternal(newChild: Expression): Expression = copy(binaryFileContent = newChild)

    override def nullSafeEval(input: Any): Any = {
        val bytes = input.asInstanceOf[Array[Byte]]
        val parser = new CDMParser(bytes)
        parser.description.serialize
    }

}
