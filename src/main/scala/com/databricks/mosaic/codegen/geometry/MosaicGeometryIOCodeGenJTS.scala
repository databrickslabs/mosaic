package com.databricks.mosaic.codegen.geometry
import com.databricks.mosaic.core.geometry.api.GeometryAPI
import com.databricks.mosaic.core.types.{BoundaryType, HolesType}
import com.databricks.mosaic.core.types.model.GeometryTypeEnum.{LINESTRING, MULTILINESTRING, MULTIPOINT, MULTIPOLYGON, POINT, POLYGON}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext}
import org.apache.spark.sql.types.{BinaryType, IntegerType, StringType}
import org.locationtech.jts.io.{WKBReader, WKTReader}
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory, LineString, Point, Polygon}
import org.locationtech.jts.io.geojson.GeoJsonReader

object MosaicGeometryIOCodeGenJTS extends GeometryIOCodeGen {

  override def fromWKT(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
    val inputGeom = ctx.freshName("inputGeom")
    val stringJavaType = CodeGenerator.javaType(StringType)
    val jtsGeom = classOf[Geometry].getName
    val wktReader = classOf[WKTReader].getName
    (s"""$jtsGeom $inputGeom = new $wktReader().read(($stringJavaType)($eval));""", inputGeom)
  }

  override def fromWKB(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
    val inputGeom = ctx.freshName("inputGeom")
    val binaryJavaType = CodeGenerator.javaType(BinaryType)
    val jtsGeom = classOf[Geometry].getName
    val wkbReader = classOf[WKBReader].getName
    (s"""$jtsGeom $inputGeom = new $wkbReader().read(($binaryJavaType)($eval));""", inputGeom)
  }

  override def fromJSON(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
    val inputGeom = ctx.freshName("inputGeom")
    val stringJavaType = CodeGenerator.javaType(StringType)
    val tmpHolder = ctx.freshName("tmpHolder")
    val jtsGeom = classOf[Geometry].getName
    val jsonReader = classOf[GeoJsonReader].getName
    (s"""
        |$stringJavaType $tmpHolder = ${CodeGenerator.getValue(eval, StringType, "0")};
        |$jtsGeom $inputGeom = new $jsonReader().read($tmpHolder);
        |$tmpHolder = null;
        |""".stripMargin, inputGeom)
  }

  override def fromHex(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {
    val inputGeom = ctx.freshName("inputGeom")
    val stringJavaType = CodeGenerator.javaType(StringType)
    val tmpHolder = ctx.freshName("tmpHolder")
    val bytes = ctx.freshName("bytes")
    val wkbReader = classOf[WKBReader].getName
    val jtsGeom = classOf[Geometry].getName
    (s"""
        |$stringJavaType $tmpHolder = ${CodeGenerator.getValue(eval, StringType, "0")};
        |byte[] $bytes = $wkbReader.hexToBytes($tmpHolder);
        |$jtsGeom $inputGeom = new $wkbReader().read($eval);
        |$bytes = null;
        |$tmpHolder = null;
        |""".stripMargin, inputGeom)
  }

  //noinspection DuplicatedCode
  override def fromInternal(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = {

    val intJavaType = CodeGenerator.javaType(IntegerType)
    val boundaryJavaType = CodeGenerator.javaType(BoundaryType)
    val holesJavaType = CodeGenerator.javaType(HolesType)

    val jtsGeometryFactory = classOf[GeometryFactory].getName
    val jtsGeometry = classOf[Geometry].getName
    val jtsPoint = classOf[Point].getName
    val jtsCoordinate = classOf[Coordinate].getName
    val jtsLineString = classOf[LineString].getName
    val jtsPolygon = classOf[Polygon].getName

    val geometryFactory = ctx.freshName("geometryFactory")
    val localGeometryFactory = ctx.freshName("geometryFactory")
    val geomTypeId = ctx.freshName("geomTypeId")
    val boundaries = ctx.freshName("boundaries")
    val boundary = ctx.freshName("boundary")
    val holes = ctx.freshName("holes")
    val localHoles = ctx.freshName("localHoles")
    val geometry = ctx.freshName("geometry")
    val coords = ctx.freshName("coords")
    val point = ctx.freshName("point")
    val points = ctx.freshName("points")
    val coordArray = ctx.freshName("coordArray")
    val lineStrings = ctx.freshName("lineStrings")
    val shell = ctx.freshName("shell")
    val holeArray = ctx.freshName("holeArray")
    val polygons = ctx.freshName("polygons")

    val constructPointFuncName = ctx.freshName("constructPointFunc")
    val constructLineStringFuncName = ctx.freshName("constructLineStringFunc")
    val constructPolygonFuncName = ctx.freshName("constructPolygonFunc")

    val constructPointFunc = ctx.addNewFunction(
      constructPointFuncName,
      s"""
         |private $jtsPoint $constructPointFuncName(double[] $coords, $jtsGeometryFactory $localGeometryFactory) {
         |  $jtsPoint $point;
         |  if ($coords.length == 2) {
         |    $point = $localGeometryFactory.createPoint(new $jtsCoordinate($coords[0], $coords[1]));
         |  } else {
         |    $point = $localGeometryFactory.createPoint(new $jtsCoordinate($coords[0], $coords[1], $coords[2]));
         |  }
         |  return $point;
         |}
         |""".stripMargin
    )

    val constructLineStringFunc = ctx.addNewFunction(
      constructLineStringFuncName,
      s"""
         |private $jtsLineString $constructLineStringFuncName(double[][] $coords, $jtsGeometryFactory $localGeometryFactory) {
         |  $jtsCoordinate[] $coordArray = new $jtsCoordinate[$coords.length];
         |  for(int i=0; i<$coords.length; i++) {
         |    $coordArray[i] = new $jtsCoordinate($coords[i][0], $coords[i][)
         |    if ($coords[i].length == 2) {
         |      $coordArray[i] = new $jtsCoordinate($coords[i][0], $coords[i][1]);
         |    } else {
         |      $coordArray[i] = new $jtsCoordinate($coords[i][0], $coords[i][1], $coords[i][2]);
         |    }
         |  }
         |  return $geometryFactory.createLineString($coordArray);
         |}
         |""".stripMargin
    )

    val constructPolygonFunc = ctx.addNewFunction(
      constructPolygonFuncName,
      s"""
         |private $jtsLineString $constructPolygonFuncName(
         |  double[][] $boundary, double[][][] $localHoles, $jtsGeometryFactory $localGeometryFactory
         |) {
         |  $jtsLineString $shell = $constructLineStringFunc($boundary, $localGeometryFactory);
         |  $jtsLineString $holeArray = new $jtsLineString[$localHoles.length];
         |  for(int i=0; i<$localHoles.length; i++) {
         |    $holeArray[i] = $constructLineStringFunc($localHoles[i], $localGeometryFactory);
         |  }
         |  return $geometryFactory.createPolygon($shell, $holeArray);
         |}
         |""".stripMargin
    )

    (s"""
        |$jtsGeometryFactory $geometryFactory = new $jtsGeometryFactory();
        |$intJavaType $geomTypeId = ${CodeGenerator.getValue(eval, IntegerType, "0")};
        |$boundaryJavaType $boundaries = ${CodeGenerator.getValue(eval, BoundaryType, "1")};
        |$holesJavaType $holes = ${CodeGenerator.getValue(eval, HolesType, "2")};
        |$jtsGeometry $geometry;
        |switch ($geomTypeId) {
        |  case ${POINT.id}: {
        |    $geometry = $constructPointFunc($boundaries[0][0], $geometryFactory);
        |    break;
        |  }
        |  case ${MULTIPOINT.id}: {
        |    $jtsPoint[] $points = new $jtsPoint[$boundaries[0].length];
        |    for(int i=0; i<$boundaries[0].length; i++) {
        |      $points[i] = $constructPointFunc($boundaries[0][i], $geometryFactory));
        |    }
        |    $geometry = $geometryFactory.createMultiPoint($points);
        |    $points = null;
        |    break;
        |  }
        |  case ${LINESTRING.id}: {
        |    $geometry = $constructLineStringFunc($boundaries[0], $geometryFactory);
        |    break;
        |  }
        |  case ${MULTILINESTRING.id}: {
        |    $jtsLineString[] $lineStrings = new $jtsLineString[$boundaries.length];
        |    for(int i=0; i<$boundaries.length; i++) {
        |      $lineStrings[i] = $constructLineStringFunc($boundaries[i], $geometryFactory);
        |    }
        |    $geometry = $geometryFactory.createMultiLineString($lineStrings);
        |    $lineStrings = null;
        |    break;
        |  }
        |  case ${POLYGON.id}: {
        |    $geometry = $constructPolygonFunc($boundaries[0], $holes[0], $geometryFactory);
        |    break;
        |  }
        |  case ${MULTIPOLYGON.id}: {
        |    $jtsPolygon[] $polygons = new $jtsPolygon[$boundaries.length];
        |    for(int i=0; i<$boundaries.length; i++) {
        |      $polygons = $constructPolygonFunc()
        |    }
        |    $geometry = $geometryFactory.createMultiPolygon($polygons);
        |    $polygons = null;
        |    break;
        |  }
        |}
        |$boundaries null;
        |$holes = null;
        |""".stripMargin, geometry)
  }

  override def toWKT(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = ???

  override def toWKB(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = ???

  override def toJSON(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = ???

  override def toHEX(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = ???

  override def toInternal(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String) = ???
}
