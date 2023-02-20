package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.expressions.base.WithExpressionInfo
import com.databricks.labs.mosaic.expressions.geometry.base.UnaryVectorExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types.{DataType, DoubleType}

case class ST_MinMaxXYZ(
    inputGeom: Expression,
    expressionConfig: MosaicExpressionConfig,
    dimension: String,
    func: String
) extends UnaryVectorExpression[ST_MinMaxXYZ](inputGeom, returnsGeometry = false, expressionConfig) {

    override def dataType: DataType = DoubleType

    override def makeCopy(newArgs: Array[AnyRef]): Expression =
        ST_MinMaxXYZ(
          newArgs(0).asInstanceOf[Expression],
          expressionConfig,
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

    class ST_XMin(override val inputGeom: Expression, override val expressionConfig: MosaicExpressionConfig)
        extends ST_MinMaxXYZ(inputGeom, expressionConfig, "x", "min")

    class ST_XMax(override val inputGeom: Expression, override val expressionConfig: MosaicExpressionConfig)
        extends ST_MinMaxXYZ(inputGeom, expressionConfig, "x", "max")

    class ST_YMin(override val inputGeom: Expression, override val expressionConfig: MosaicExpressionConfig)
        extends ST_MinMaxXYZ(inputGeom, expressionConfig, "y", "min")

    class ST_YMax(override val inputGeom: Expression, override val expressionConfig: MosaicExpressionConfig)
        extends ST_MinMaxXYZ(inputGeom, expressionConfig, "y", "max")

    class ST_ZMin(override val inputGeom: Expression, override val expressionConfig: MosaicExpressionConfig)
        extends ST_MinMaxXYZ(inputGeom, expressionConfig, "z", "min")

    class ST_ZMax(override val inputGeom: Expression, override val expressionConfig: MosaicExpressionConfig)
        extends ST_MinMaxXYZ(inputGeom, expressionConfig, "z", "max")

    object ST_XMin extends WithExpressionInfo {

        override def name: String = "st_xmin"
        override def usage: String = "_FUNC_(expr1) - Returns min x coordinate of a geometry."
        override def example: String =
            """
              |    Examples:
              |      > SELECT _FUNC_(a);
              |        12.3
              |  """.stripMargin
        override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = { (exprs: Seq[Expression]) =>
            ST_MinMaxXYZ(exprs.head, expressionConfig, "x", "min")
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
        override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = { (exprs: Seq[Expression]) =>
            ST_MinMaxXYZ(exprs.head, expressionConfig, "x", "max")
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
        override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = { (exprs: Seq[Expression]) =>
            ST_MinMaxXYZ(exprs.head, expressionConfig, "y", "min")
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
        override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = { (exprs: Seq[Expression]) =>
            ST_MinMaxXYZ(exprs.head, expressionConfig, "y", "max")
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
        override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = { (exprs: Seq[Expression]) =>
            ST_MinMaxXYZ(exprs.head, expressionConfig, "z", "min")
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
        override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = { (exprs: Seq[Expression]) =>
            ST_MinMaxXYZ(exprs.head, expressionConfig, "z", "max")
        }

    }

}
