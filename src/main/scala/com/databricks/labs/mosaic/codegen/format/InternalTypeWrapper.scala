package com.databricks.labs.mosaic.codegen.format

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}

object InternalTypeWrapper {

    def doGenCode(
        ctx: CodegenContext,
        ev: ExprCode,
        nullSafeCodeGen: (
            CodegenContext,
            ExprCode,
            String => String
        ) => ExprCode
    ): ExprCode = {
        val rowClass = classOf[GenericInternalRow].getName
        val values = ctx.freshName("values")
        nullSafeCodeGen(
          ctx,
          ev,
          eval => {
              s"""
                 |Object[] $values = new Object[1];
                 |$values[0] = $eval;
                 |${ev.value} = new $rowClass($values);
                 |$values = null;
                 |""".stripMargin
          }
        )
    }
}
