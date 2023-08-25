package com.databricks.labs.mosaic.core.codegen.format

import com.databricks.labs.mosaic.core.expressions.geometry.RequiresCRS
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.types.{GeoJSONType, HexType}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.types.{BinaryType, CalendarIntervalType, StringType}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class ConvertToCodeGenTest extends AnyFunSuite with MockFactory {

  val mockGeometryAPI: GeometryAPI = mock[GeometryAPI]
  val mockIO: GeometryIOCodeGen = mock[GeometryIOCodeGen]
  val mocCtx: CodegenContext = mock[CodegenContext]

  def doMock(): Unit = {
    mockIO.fromWKT _ expects(mocCtx, "eval1", mockGeometryAPI) returning(
      "Geometry geom1 = new Geometry(eval1.toString());", "geom1"
    ) anyNumberOfTimes()
    mockIO.fromWKB _ expects(mocCtx, "eval1", mockGeometryAPI) returning(
      "Geometry geom1 = new Geometry(eval1.bytes());", "geom1"
    ) anyNumberOfTimes()
    mockIO.fromHex _ expects(mocCtx, "eval1", mockGeometryAPI) returning(
      "Geometry geom1 = new Geometry(eval1.hex().toBytes());", "geom1"
    ) anyNumberOfTimes()
    mockIO.fromGeoJSON _ expects(mocCtx, "eval1", mockGeometryAPI) returning(
      "Geometry geom1 = Geometry.parseJSON(eval1);", "geom1"
    ) anyNumberOfTimes()

    mockIO.toWKT _ expects(mocCtx, "geom1", mockGeometryAPI) returning(
      "String wkt2 = geom1.toWKT();", "wkt2"
    ) anyNumberOfTimes()
    mockIO.toWKB _ expects(mocCtx, "geom1", mockGeometryAPI) returning(
      "byte[] wkb2 = geom1.toWKB();", "wkb2"
    ) anyNumberOfTimes()
    mockIO.toHEX _ expects(mocCtx, "geom1", mockGeometryAPI) returning(
      "String hex2 = geom1.toHex();", "hex2"
    ) anyNumberOfTimes()
    mockIO.toGeoJSON _ expects(mocCtx, "geom1", mockGeometryAPI) returning(
      "String json2 = geom1.toJSON();", "json2"
    ) anyNumberOfTimes()

    mockGeometryAPI.ioCodeGen _ expects() returning mockIO anyNumberOfTimes()
  }

  test("ConvertToCodeGen should generate read code") {
    doMock()

    ConvertToCodeGen.readGeometryCode(
      mocCtx, "eval1", StringType, mockGeometryAPI
    ) shouldEqual mockIO.fromWKT(mocCtx, "eval1", mockGeometryAPI)

    ConvertToCodeGen.readGeometryCode(
      mocCtx, "eval1", BinaryType, mockGeometryAPI
    ) shouldEqual mockIO.fromWKB(mocCtx, "eval1", mockGeometryAPI)

    ConvertToCodeGen.readGeometryCode(
      mocCtx, "eval1", HexType, mockGeometryAPI
    ) shouldEqual mockIO.fromHex(mocCtx, "eval1", mockGeometryAPI)

    ConvertToCodeGen.readGeometryCode(
      mocCtx, "eval1", GeoJSONType, mockGeometryAPI
    ) shouldEqual mockIO.fromGeoJSON(mocCtx, "eval1", mockGeometryAPI)

    an[Error] should be thrownBy ConvertToCodeGen.readGeometryCode(
      mocCtx, "eval1", CalendarIntervalType, mockGeometryAPI
    )
  }

  test("ConvertToCodeGen should generate write code") {
    doMock()

    ConvertToCodeGen.writeGeometryCode(
      mocCtx, "geom1", StringType, mockGeometryAPI
    ) shouldEqual mockIO.toWKT(mocCtx, "geom1", mockGeometryAPI)

    ConvertToCodeGen.writeGeometryCode(
      mocCtx, "geom1", BinaryType, mockGeometryAPI
    ) shouldEqual mockIO.toWKB(mocCtx, "geom1", mockGeometryAPI)

    ConvertToCodeGen.writeGeometryCode(
      mocCtx, "geom1", HexType, mockGeometryAPI
    ) shouldEqual mockIO.toHEX(mocCtx, "geom1", mockGeometryAPI)

    ConvertToCodeGen.writeGeometryCode(
      mocCtx, "geom1", "GEOJSON", mockGeometryAPI
    ) shouldEqual mockIO.toGeoJSON(mocCtx, "geom1", mockGeometryAPI)

    an[Error] should be thrownBy ConvertToCodeGen.writeGeometryCode(
      mocCtx, "eval1", "other", mockGeometryAPI
    )
  }

  test("ConvertToCodeGen should generate code for different input and output types") {
    doMock()

    // Cannot mock due to inheritance issues
    val valueEv = VariableValue("eval3", null)
    val evCode = ExprCode(null, valueEv)

    val expectedCode: String =
      s"""
         |Geometry geom1 = new Geometry(eval1.toString());
         |byte[] wkb2 = geom1.toWKB();
         |eval3 = wkb2;
         |""".stripMargin

    mockGeometryAPI.codeGenTryWrap _ expects expectedCode returning
      s"""try{$expectedCode}""" anyNumberOfTimes()


    val result = ConvertToCodeGen.fromEval(
      mocCtx,
      evCode,
      "eval1",
      StringType,
      "WKB",
      mockGeometryAPI
    )

    result.contains(expectedCode) shouldBe true
    result.contains("try") && result.contains("{") && result.contains("}") shouldBe true

    val nullSafeWrapper: (CodegenContext, ExprCode, String => String) => ExprCode = {
      (_: CodegenContext, _: ExprCode, _: String => String) => {
        val code = ConvertToCodeGen.fromEval(
          mocCtx,
          evCode,
          "eval1",
          StringType,
          "WKB",
          mockGeometryAPI)
        ExprCode(null, VariableValue(code, null))
      }
    }

    val codeGen = ConvertToCodeGen.doCodeGen(
      mocCtx,
      evCode,
      nullSafeWrapper,
      StringType,
      "WKB",
      mockGeometryAPI
    )

    codeGen.value.code.contains(expectedCode) shouldBe true

  }

  test("ConvertToCodeGen should generate code for same input and output types") {
    doMock()

    // Cannot mock due to inheritance issues
    val valueEv = VariableValue("eval2", null)
    val evCode = ExprCode(null, valueEv)


    val expectedCode: String =
      s"""
         |eval2 = eval1;
         |""".stripMargin


    val result = ConvertToCodeGen.fromEval(
      mocCtx,
      evCode,
      "eval1",
      BinaryType,
      "binary",
      mockGeometryAPI
    )

    result.contains(expectedCode) shouldBe true
    !result.contains("try") && !result.contains("{") && !result.contains("}") shouldBe true

    val nullSafeWrapper: (CodegenContext, ExprCode, String => String) => ExprCode = {
      (_: CodegenContext, _: ExprCode, _: String => String) => ExprCode(null, VariableValue(expectedCode, null))
    }

    val codeGen = ConvertToCodeGen.doCodeGen(
      mocCtx,
      evCode,
      nullSafeWrapper,
      BinaryType,
      "WKB",
      mockGeometryAPI
    )

    codeGen.value.code.contains(expectedCode) shouldBe true
  }

  test("RequiresCRS should return correct encoding for each geometry type") {
    doMock()

    object TestObject extends RequiresCRS {}

    noException should be thrownBy TestObject.checkEncoding(GeoJSONType)
    an[Exception] should be thrownBy TestObject.checkEncoding(StringType)
  }

}
