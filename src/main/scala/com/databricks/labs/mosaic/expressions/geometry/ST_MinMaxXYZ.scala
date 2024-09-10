package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.expressions.base.WithExpressionInfo
import com.databricks.labs.mosaic.expressions.geometry.base.UnaryVectorExpression
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types.{DataType, DoubleType}

case class ST_MinMaxXYZ(
                           inputGeom: Expression,
                           exprConfig: ExprConfig,
                           dimension: String,
                           func: String
) extends UnaryVectorExpression[ST_MinMaxXYZ](inputGeom, returnsGeometry = false, exprConfig) {

    override def dataType: DataType = DoubleType

    override def makeCopy(newArgs: Array[AnyRef]): Expression =
        ST_MinMaxXYZ(
          newArgs(0).asInstanceOf[Expression],
          exprConfig,
          dimension,
          func
        )

    override def geometryTransform(geometry: MosaicGeometry): Any = geometry.minMaxCoord(dimension, func)

    override def geometryCodeGen(geometryRef: String, ctx: CodegenContext): (String, String) = {
        val resultRef = ctx.freshName("result")
        val code = s"""double $resultRef = $geometryRef.minMaxCoord("$dimension", "$func");"""
        (code, resultRef)
    }

}

/** Expression info required for the expression registration for spark SQL. */
object ST_MinMaxXYZ {

    class ST_XMin(override val inputGeom: Expression, override val exprConfig: ExprConfig)
        extends ST_MinMaxXYZ(inputGeom, exprConfig, "x", "min")

    class ST_XMax(override val inputGeom: Expression, override val exprConfig: ExprConfig)
        extends ST_MinMaxXYZ(inputGeom, exprConfig, "x", "max")

    class ST_YMin(override val inputGeom: Expression, override val exprConfig: ExprConfig)
        extends ST_MinMaxXYZ(inputGeom, exprConfig, "y", "min")

    class ST_YMax(override val inputGeom: Expression, override val exprConfig: ExprConfig)
        extends ST_MinMaxXYZ(inputGeom, exprConfig, "y", "max")

    class ST_ZMin(override val inputGeom: Expression, override val exprConfig: ExprConfig)
        extends ST_MinMaxXYZ(inputGeom, exprConfig, "z", "min")

    class ST_ZMax(override val inputGeom: Expression, override val exprConfig: ExprConfig)
        extends ST_MinMaxXYZ(inputGeom, exprConfig, "z", "max")

    object ST_XMin extends WithExpressionInfo {

        override def name: String = "st_xmin"
        override def usage: String = "_FUNC_(expr1) - Returns min x coordinate of a geometry."
        override def example: String =
            """
              |    Examples:
              |      > SELECT _FUNC_(a);
              |        12.3
              |  """.stripMargin
        override def builder(exprConfig: ExprConfig): FunctionBuilder = { (exprs: Seq[Expression]) =>
            ST_MinMaxXYZ(exprs.head, exprConfig, "x", "min")
        }

    }

    object ST_XMax extends WithExpressionInfo {

        override def name: String = "st_xmax"
        override def usage: String = "_FUNC_(expr1) - Returns max x coordinate of a geometry."
        override def example: String =
            """
              |    Examples:
              |      > SELECT _FUNC_(a);
              |        12.3
              |  """.stripMargin
        override def builder(exprConfig: ExprConfig): FunctionBuilder = { (exprs: Seq[Expression]) =>
            ST_MinMaxXYZ(exprs.head, exprConfig, "x", "max")
        }

    }

    object ST_YMin extends WithExpressionInfo {

        override def name: String = "st_ymin"
        override def usage: String = "_FUNC_(expr1) - Returns min y coordinate of a geometry."
        override def example: String =
            """
              |    Examples:
              |      > SELECT _FUNC_(a);
              |        12.3
              |  """.stripMargin
        override def builder(exprConfig: ExprConfig): FunctionBuilder = { (exprs: Seq[Expression]) =>
            ST_MinMaxXYZ(exprs.head, exprConfig, "y", "min")
        }

    }

    object ST_YMax extends WithExpressionInfo {

        override def name: String = "st_ymax"
        override def usage: String = "_FUNC_(expr1) - Returns max y coordinate of a geometry."
        override def example: String =
            """
              |    Examples:
              |      > SELECT _FUNC_(a);
              |        12.3
              |  """.stripMargin
        override def builder(exprConfig: ExprConfig): FunctionBuilder = { (exprs: Seq[Expression]) =>
            ST_MinMaxXYZ(exprs.head, exprConfig, "y", "max")
        }

    }

    object ST_ZMin extends WithExpressionInfo {

        override def name: String = "st_zmin"
        override def usage: String = "_FUNC_(expr1) - Returns min z coordinate of a geometry."
        override def example: String =
            """
              |    Examples:
              |      > SELECT _FUNC_(a);
              |        12.3
              |  """.stripMargin
        override def builder(exprConfig: ExprConfig): FunctionBuilder = { (exprs: Seq[Expression]) =>
            ST_MinMaxXYZ(exprs.head, exprConfig, "z", "min")
        }

    }

    object ST_ZMax extends WithExpressionInfo {

        override def name: String = "st_zmax"
        override def usage: String = "_FUNC_(expr1) - Returns max z coordinate of a geometry."
        override def example: String =
            """
              |    Examples:
              |      > SELECT _FUNC_(a);
              |        12.3
              |  """.stripMargin
        override def builder(exprConfig: ExprConfig): FunctionBuilder = { (exprs: Seq[Expression]) =>
            ST_MinMaxXYZ(exprs.head, exprConfig, "z", "max")
        }

    }

}
