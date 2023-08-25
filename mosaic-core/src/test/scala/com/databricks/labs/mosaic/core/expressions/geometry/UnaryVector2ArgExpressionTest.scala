package com.databricks.labs.mosaic.core.expressions.geometry

import com.databricks.labs.mosaic.core.codegen.format.GeometryIOCodeGen
import com.databricks.labs.mosaic.core.expressions.MosaicExpressionConfig
import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, VariableValue}
import org.apache.spark.sql.catalyst.expressions.{Conv, Expression}
import org.apache.spark.sql.types.BinaryType
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class UnaryVector2ArgExpressionTest extends AnyFunSuite with MockFactory {

  val mockFirstExpression: Expression = mock[Expression]
  val mockSecondExpression: Expression = mock[Expression]
  val mockThirdExpression: Expression = mock[Expression]
  val mockGeometryAPI: GeometryAPI = mock[GeometryAPI]
  val mockExpressionConfig: MosaicExpressionConfig = mock[MosaicExpressionConfig]

  // Mocking doesn't work well with templates, so we create a dummy class to extend the abstract class
  // We are using Abs as a template in order to test makeCopy which is linked to GenericExpressionFactory
  abstract class DummyExpr extends UnaryVector2ArgExpression[Conv](
    mockFirstExpression, mockSecondExpression, mockThirdExpression, true, mockExpressionConfig
  ) {
    // For partial mocking, make methods that are testable final, scalamock will not mock final methods
    override final def nullSafeEval(leftGeometryRow: Any, arg1Row: Any, arg2Row: Any): Any =
      super.nullSafeEval(leftGeometryRow, arg1Row, arg2Row)

    override final def first: Expression = super.first

    override final def second: Expression = super.second

    override final def third: Expression = super.third

    override final def geometryAPI: GeometryAPI = super.geometryAPI

    override final def makeCopy(newArgs: Array[AnyRef]): Expression = super.makeCopy(newArgs)

    override final def withNewChildrenInternal(newFirst: Expression, newSecond: Expression, newThird: Expression): Expression =
      super.withNewChildrenInternal(newFirst, newSecond, newThird)

    override final def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = super.doGenCode(ctx, ev)

    // We are making inherited nullSafeCodeGen final and passthrough so that we can test the nested behavior in doGenCode
    override final def nullSafeCodeGen(ctx: CodegenContext, ev: ExprCode, f: (String, String, String) => String): ExprCode = {
      ExprCode(null, VariableValue(f("geom1", "arg1", "arg2"), null))
    }

  }

  val bytes: Array[Byte] = "POINT EMPTY".getBytes

  val mockExpression: DummyExpr = mock[DummyExpr]
  val mockIndexSystem: IndexSystem = mock[IndexSystem]
  val mockPoint: MosaicGeometry = mock[MosaicGeometry]
  val mockCtx: CodegenContext = mock[CodegenContext]
  val mockIO: GeometryIOCodeGen = mock[GeometryIOCodeGen]

  val expectedCode: String =
    """
      |Geometry geom1 = Geometry(wkb1);
      |MosaicGeometry geom3 = MosaicGeometry(geom1).buffer(arg1 + arg2);
      |byte[] wkb3 = geom3.toWKB();
      |eval1 = wkb3;
      |""".stripMargin

  def doMock(): Unit = {
    mockExpressionConfig.getGeometryAPI _ expects * returning mockGeometryAPI anyNumberOfTimes()

    mockFirstExpression.dataType _ expects() returning BinaryType anyNumberOfTimes()

    mockGeometryAPI.valueToGeometry _ expects(bytes, mockFirstExpression.dataType) returning mockPoint anyNumberOfTimes()
    mockGeometryAPI.ioCodeGen _ expects() returning mockIO anyNumberOfTimes()
    mockGeometryAPI.codeGenTryWrap _ expects expectedCode returning expectedCode anyNumberOfTimes()

    mockIO.fromWKB _ expects(mockCtx, "geom1", mockGeometryAPI) returning("Geometry geom1 = Geometry(wkb1);", "geom1") anyNumberOfTimes()

    mockExpression.geometryTransform _ expects (*, *, *) returning mockPoint anyNumberOfTimes()
    mockExpression.serialise _ expects(mockPoint, true, BinaryType) returning bytes anyNumberOfTimes()
    mockExpression.mosaicGeometryRef _ expects "geom1" returning "MosaicGeometry(geom1)" anyNumberOfTimes()
    mockExpression.geometryCodeGen _ expects(*, *, *, *) returning("MosaicGeometry geom3 = MosaicGeometry(geom1).buffer(arg1 + arg2);", "geom3") anyNumberOfTimes()
    mockExpression.serialiseCodegen _ expects(*, *, *, *) returning("byte[] wkb3 = geom3.toWKB();", "wkb3") anyNumberOfTimes()

  }


  test("BinaryVectorExpression should implement accessor methods") {
    doMock()

    mockExpression.first shouldBe mockFirstExpression
    mockExpression.second shouldBe mockSecondExpression
    mockExpression.third shouldBe mockThirdExpression
    mockExpression.geometryAPI shouldBe mockGeometryAPI
    mockExpression.makeCopy(Array(mockFirstExpression, mockSecondExpression, mockThirdExpression)) shouldBe
      Conv(mockFirstExpression, mockSecondExpression, mockThirdExpression)
    mockExpression.withNewChildrenInternal(mockFirstExpression, mockSecondExpression, mockThirdExpression) shouldBe
      Conv(mockFirstExpression, mockSecondExpression, mockThirdExpression)
  }

  test("VectorExpression should evaluate") {
    doMock()

    val result = mockExpression.nullSafeEval(bytes, 1, 2)

    result shouldBe bytes
  }

  test("VectorExpression should doGenCode") {
    doMock()

    val exprCode = ExprCode(null, VariableValue("eval1", null))
    mockExpression.doGenCode(mockCtx, exprCode).value.code shouldBe expectedCode
  }


}
