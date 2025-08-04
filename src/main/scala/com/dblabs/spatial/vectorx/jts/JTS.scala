package com.dblabs.spatial.vectorx.jts

import org.locationtech.jts.geom.util.AffineTransformation
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory, LineString, Point, Polygon}
import org.locationtech.jts.io.{WKBReader, WKBWriter, WKTReader, WKTWriter}

import scala.collection.mutable

object JTS {

    private val geometryFactory = new GeometryFactory()
    private val wkbReader = new WKBReader(geometryFactory)
    private val wkbWriter = new WKBWriter()
    private val wktReader = new WKTReader(geometryFactory)
    private val wktWriter = new WKTWriter()

    def point(x: Double, y: Double): Point = {
        geometryFactory.createPoint(new Coordinate(x, y))
    }

    def point(coordinate: Coordinate): Point = {
        geometryFactory.createPoint(coordinate)
    }

    def polygonFromPoints(points: Array[Point]): Polygon = {
        geometryFactory.createPolygon(
          points.map(_.getCoordinate)
        )
    }

    def polygonFromCoords(coordinates: Array[Coordinate]): Polygon = {
        geometryFactory.createPolygon(
          coordinates
        )
    }

    def polygonFromXYs(xys: Array[(Double, Double)]): Polygon = {
        val coordinates = xys.map { case (x, y) => new Coordinate(x, y) }
        geometryFactory.createPolygon(coordinates)
    }

    def coordinatesFromXYs(getX: Double, getY: Double): Coordinate = {
        new Coordinate(getX, getY)
    }

    def lineStringXYs(xys: mutable.Buffer[(Double, Double)]): LineString = {
        val coordinates = xys.map { case (x, y) => new Coordinate(x, y) }.toArray
        geometryFactory.createLineString(coordinates)
    }

    def translate(xd: Double, yd: Double, geometry: Geometry): Geometry = {
        val transformation = AffineTransformation.translationInstance(xd, yd)
        transformation.transform(geometry)
    }

    def fromWKB(bytes: Array[Byte]): Geometry = {
        wkbReader.read(bytes)
    }

    def emptyPolygon: Geometry = JTS.fromWKT("POLYGON EMPTY")

    def toWKB(intersection: Geometry): Array[Byte] = {
        wkbWriter.write(intersection)
    }

    def fromWKT(wkt: String): Geometry = {
        wktReader.read(wkt)
    }

    def toWKT(geometry: Geometry): String = {
        wktWriter.write(geometry)
    }

}
