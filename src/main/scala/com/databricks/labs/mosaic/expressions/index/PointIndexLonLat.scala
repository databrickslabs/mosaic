package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.index.IndexSystemID
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class PointIndexLonLat(lon: Expression, lat: Expression, resolution: Expression, idAsLong: Expression, indexSystemName: String)
    extends QuaternaryExpression
      with ExpectsInputTypes
      with NullIntolerant
      with CodegenFallback {

    override def inputTypes: Seq[DataType] =
        (lon.dataType, lat.dataType, resolution.dataType, idAsLong.dataType) match {
            case (DoubleType, DoubleType, IntegerType, BooleanType) => Seq(DoubleType, DoubleType, IntegerType, BooleanType)
            case (DoubleType, DoubleType, StringType, BooleanType)  => Seq(DoubleType, DoubleType, StringType, BooleanType)
            case _                                                  => throw new Error(
                  s"Not supported data type: (${lon.dataType}, ${lat.dataType}, ${resolution.dataType}, ${idAsLong.dataType})."
                )
        }

    /** Expression output DataType. */
    override def dataType: DataType =
        idAsLong match {
            case Literal(f: Boolean, BooleanType) => if (f) LongType else StringType
            case _                                => throw new Error("idAsLong has to be Boolean type.")
        }

    override def toString: String = s"grid_longlatascellid($lon, $lat, $resolution)"

    /** Overridden to ensure [[Expression.sql]] is properly formatted. */
    override def prettyName: String = "grid_longlatascellid"

    /**
      * Computes the index corresponding to the provided lat and long
      * coordinates.
      *
      * @param input1
      *   Any instance containing longitude.
      * @param input2
      *   Any instance containing latitude.
      * @param input3
      *   Any instance containing resolution.
      * @return
      *   Index id in Long.
      */
    override def nullSafeEval(input1: Any, input2: Any, input3: Any, input4: Any): Any = {
        val indexSystem = IndexSystemID.getIndexSystem(IndexSystemID(indexSystemName))
        val resolution: Int = indexSystem.getResolution(input3)
        val lon: Double = input1.asInstanceOf[Double]
        val lat: Double = input2.asInstanceOf[Double]
        val idAsLongVal = input4.asInstanceOf[Boolean]

        val cellID = indexSystem.pointToIndex(lon, lat, resolution)
        if (idAsLongVal) cellID else UTF8String.fromString(indexSystem.format(cellID))
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(4).map(_.asInstanceOf[Expression])
        val res = PointIndexLonLat(asArray(0), asArray(1), asArray(2), asArray(3), indexSystemName)
        res.copyTagsFrom(this)
        res
    }

    override def first: Expression = lon

    override def second: Expression = lat

    override def third: Expression = resolution

    override def fourth: Expression = idAsLong

    override protected def withNewChildrenInternal(
        newFirst: Expression,
        newSecond: Expression,
        newThird: Expression,
        newFourth: Expression
    ): Expression = copy(newFirst, newSecond, newThird, newFourth)

}

object PointIndexLonLat {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[PointIndexLonLat].getCanonicalName,
          db.orNull,
          "grid_longlatascellid",
          """
            |    _FUNC_(lon, lat, resolution) - Returns the index of a point(lon, lat) at resolution.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a, b, 10);
            |        622236721348804607
            |  """.stripMargin,
          "",
          "misc_funcs",
          "1.0",
          "",
          "built-in"
        )

}
