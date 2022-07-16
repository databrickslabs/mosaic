package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.codegen.format.ConvertToCodeGen
import com.databricks.labs.mosaic.codegen.geometry.CentroidCodeGen
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types._

case class ST_Centroid(inputGeom: Expression, geometryAPIName: String, nDim: Int = 2) extends UnaryExpression with NullIntolerant {

    /**
      * ST_Centroid expression returns the centroid of the
      * [[org.locationtech.jts.geom.Geometry]] instance extracted from inputGeom
      * expression.
      */

    override def child: Expression = inputGeom

    /** Output Data Type */
    // scalastyle:off throwerror
    override def dataType: DataType =
        nDim match {
            case 2 => StructType(Seq(StructField("x", DoubleType), StructField("y", DoubleType)))
            case 3 => StructType(Seq(StructField("x", DoubleType), StructField("y", DoubleType), StructField("z", DoubleType)))
            case _ => throw new NotImplementedError("Only 2D and 3D centroid supported!")
        }
    // scalastyle:on throwerror

    override def nullSafeEval(input1: Any): Any = {
        val geometryAPI = GeometryAPI(geometryAPIName)

        val geom = geometryAPI.geometry(input1, inputGeom.dataType)
        val centroid = geom.getCentroid
        nDim match {
            case 2 => InternalRow.fromSeq(Seq(centroid.getX, centroid.getY))
            case 3 => InternalRow.fromSeq(Seq(centroid.getX, centroid.getY, centroid.getZ))
        }
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val arg1 = newArgs.head.asInstanceOf[Expression]
        val res = ST_Centroid(arg1, geometryAPIName, nDim)
        res.copyTagsFrom(this)
        res
    }

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
        nullSafeCodeGen(
          ctx,
          ev,
          eval => {
              val geometryAPI = GeometryAPI.apply(geometryAPIName)
              val (inCode, geomInRef) = ConvertToCodeGen.readGeometryCode(ctx, eval, inputGeom.dataType, geometryAPI)
              val (centroidCode, centroidRow) = CentroidCodeGen(geometryAPI).centroid(ctx, geomInRef, geometryAPI, nDim)
              geometryAPI.codeGenTryWrap(s"""
                                            |$inCode
                                            |$centroidCode
                                            |${ev.value} = $centroidRow;
                                            |""".stripMargin)
          }
        )

    override protected def withNewChildInternal(newChild: Expression): Expression = copy(inputGeom = newChild)

}

object ST_Centroid {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String], name: String): ExpressionInfo =
        new ExpressionInfo(
          classOf[ST_Centroid].getCanonicalName,
          db.orNull,
          name,
          """
            |    _FUNC_(expr1) - Returns the centroid of the geometry.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a);
            |        (15.2512, 12.12)
            |  """.stripMargin,
          "",
          "misc_funcs",
          "1.0",
          "",
          "built-in"
        )

}
