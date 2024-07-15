package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineString
import com.databricks.labs.mosaic.core.geometry.multilinestring.MosaicMultiLineString
import com.databricks.labs.mosaic.core.geometry.multipoint.MosaicMultiPoint
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.{MULTIPOINT, POINT}
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.geometry.base.VectorExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression, TernaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

import java.util.Locale

case class ST_Triangulate (
                              inputPointsArray: Expression,
                              inputLinesArray: Expression,
                              inputTolerance: Expression,
                              expressionConfig: MosaicExpressionConfig
                          )
    extends CollectionGenerator
    with Serializable
    with CodegenFallback {


    override def position: Boolean = false

    override def inline: Boolean = false

    override def elementSchema: StructType = StructType(Seq(StructField("triangles", firstElementType)))

    def firstElementType: DataType = inputPointsArray.dataType.asInstanceOf[ArrayType].elementType

    def secondElementType: DataType = inputLinesArray.dataType.asInstanceOf[ArrayType].elementType

    def getGeometryAPI(expressionConfig: MosaicExpressionConfig): GeometryAPI = GeometryAPI(expressionConfig.getGeometryAPI)

    def geometryAPI: GeometryAPI = getGeometryAPI(expressionConfig)

    override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
        val pointsGeom =
            inputPointsArray
                .eval(input)
                .asInstanceOf[ArrayData]
                .toObjectArray(firstElementType)
                .map(geometryAPI.geometry(_, firstElementType))

        val multiPointGeom = geometryAPI.fromSeq(pointsGeom, MULTIPOINT).asInstanceOf[MosaicMultiPoint]
        val linesGeom = inputLinesArray.eval(input).asInstanceOf[ArrayData].toObjectArray(secondElementType).map(geometryAPI.geometry(_, secondElementType).asInstanceOf[MosaicLineString])

        val triangles =  multiPointGeom.triangulate(linesGeom, inputTolerance.eval(input).asInstanceOf[Double])

        val outputGeoms = triangles.map(
            geometryAPI.serialize(_, firstElementType)
        )
        val outputRows = outputGeoms.map(t => InternalRow.fromSeq(Seq(t)))
        outputRows
    }

    override def children: Seq[Expression] = Seq(inputPointsArray, inputLinesArray, inputTolerance)

    override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
        copy(newChildren(0), newChildren(1), newChildren(2))
}


object ST_Triangulate extends WithExpressionInfo {

    override def name: String = "st_triangulate"

    override def usage: String = "_FUNC_(expr1, expr2, expr3) - Returns the triangulated irregular network of `expr1` including `expr2` as breaklines with tolerance `expr3`."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a, b, c);
          |        MULTIPOLYGON Z (((...)))
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[ST_Triangulate](3, expressionConfig)
    }

}

/*
//    extends TernaryExpression
//    with VectorExpression
    /**
     * The function to be overriden by the extending class. It is called when
     * the expression is evaluated. It provides the vector geometry to the
     * expression. It abstracts spark serialization from the caller.
     *
     * @param points
     * The geometry.
     * @param lines
     * The first argument.
     * @param tol
     * The second argument.
     * @return
     * A result of the expression.
     */
    override def nullSafeEval(points: Any, lines: Any, tol: Any): Any = {
        val pointsGeom = geometryAPI.geometry(points, first.dataType)
        val linesGeom = geometryAPI.geometry(lines, second.dataType)
        val triangles = pointsGeom.getGeometryType.toUpperCase(Locale.ROOT) match {
            case "MULTIPOINT" =>
                pointsGeom.asInstanceOf[MosaicMultiPoint].triangulate(linesGeom.asInstanceOf[MosaicMultiLineString], tol.asInstanceOf[Double])
            case _ =>
                throw new UnsupportedOperationException("ST_Triangulate requires MULTIPOINT geometry as input")
        }
        val outputGeoms = triangles.map(
            serialise(_, returnsGeometry = true, inputPointsGeom.dataType)
        )
        ArrayData.toArrayData(outputGeoms)
    }

    override def geometryAPI: GeometryAPI = getGeometryAPI(expressionConfig)

    override def dataType: DataType = ArrayType(inputPointsGeom.dataType)

    override def first: Expression = inputPointsGeom

    override def second: Expression = inputLinesGeom

    override def third: Expression = inputTolerance

    override def prettyName: String = "st_triangulate"

    override protected def withNewChildrenInternal(newFirst: Expression, newSecond: Expression, newThird: Expression): Expression = makeCopy(Array(newFirst, newSecond, newThird))

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.map(_.asInstanceOf[Expression])
        val res = ST_Triangulate(asArray(0), asArray(1), asArray(2), expressionConfig)
        res.copyTagsFrom(this)
        res
    }
 */