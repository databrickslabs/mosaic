package com.dblabs.spatial.vectorx.jts

import org.locationtech.jts.geom.util.AffineTransformation
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory, LineString, Point, Polygon}
import org.locationtech.jts.io.{WKBReader, WKBWriter, WKTReader, WKTWriter}
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier

import scala.collection.mutable

object JTS {

    // JTS writers are not thread safe and JTS singleton is shared between all tasks within the worker node
    // We do TID based lookup maps for all reusable instances just to be on the safe side of the thread safety
    private val geometryFactories = mutable.Map[Long, GeometryFactory]()
    private val wkbReaders = mutable.Map[Long, WKBReader]()
    private val wkbWriters = mutable.Map[Long, WKBWriter]()
    private val wtkWriters = mutable.Map[Long, WKTWriter]()
    private val wtkReaders = mutable.Map[Long, WKTReader]()

    def point(x: Double, y: Double): Point = {
        val tid = Thread.currentThread().getId
        val gf = geometryFactories.getOrElseUpdate(tid, new GeometryFactory())
        gf.createPoint(new Coordinate(x, y))
    }

    def point(coordinate: Coordinate): Point = {
        val tid = Thread.currentThread().getId
        val gf = geometryFactories.getOrElseUpdate(tid, new GeometryFactory())
        gf.createPoint(coordinate)
    }

    def polygonFromPoints(points: Array[Point]): Polygon = {
        val tid = Thread.currentThread().getId
        val gf = geometryFactories.getOrElseUpdate(tid, new GeometryFactory())
        gf.createPolygon(
          points.map(_.getCoordinate)
        )
    }

    def polygonFromCoords(coordinates: Array[Coordinate]): Polygon = {
        val tid = Thread.currentThread().getId
        val gf = geometryFactories.getOrElseUpdate(tid, new GeometryFactory())
        gf.createPolygon(
          coordinates
        )
    }

    def polygonFromXYs(xys: Array[(Double, Double)]): Polygon = {
        val tid = Thread.currentThread().getId
        val gf = geometryFactories.getOrElseUpdate(tid, new GeometryFactory())
        val coordinates = xys.map { case (x, y) => new Coordinate(x, y) }
        gf.createPolygon(coordinates)
    }

    def coordinatesFromXYs(getX: Double, getY: Double): Coordinate = {
        new Coordinate(getX, getY)
    }

    def lineStringXYs(xys: mutable.Buffer[(Double, Double)]): LineString = {
        val tid = Thread.currentThread().getId
        val gf = geometryFactories.getOrElseUpdate(tid, new GeometryFactory())
        val coordinates = xys.map { case (x, y) => new Coordinate(x, y) }.toArray
        gf.createLineString(coordinates)
    }

    def translate(xd: Double, yd: Double, geometry: Geometry): Geometry = {
        val transformation = AffineTransformation.translationInstance(xd, yd)
        transformation.transform(geometry)
    }

    def fromWKB(bytes: Array[Byte]): Geometry = {
        val tid = Thread.currentThread().getId
        val reader = wkbReaders.getOrElseUpdate(tid, new WKBReader())
        reader.read(bytes)
    }

    def emptyPolygon: Geometry = JTS.fromWKT("POLYGON EMPTY")

    def toWKB(intersection: Geometry): Array[Byte] = {
        // val wkbWriter = new WKBWriter()
        val tid = Thread.currentThread().getId
        val writer = wkbWriters.getOrElseUpdate(tid, new WKBWriter())
        writer.write(intersection)
    }

    def fromWKT(wkt: String): Geometry = {
        val tid = Thread.currentThread().getId
        val reader = wtkReaders.getOrElseUpdate(tid, new WKTReader())
        reader.read(wkt)
    }

    def toWKT(geometry: Geometry): String = {
        val tid = Thread.currentThread().getId
        val writer = wtkWriters.getOrElseUpdate(tid, new WKTWriter())
        writer.write(geometry)
    }

    def simplify(geometry: Geometry, tolerance: Double): Geometry = {
        val simplified = DouglasPeuckerSimplifier.simplify(geometry, tolerance)
        simplified.setSRID(geometry.getSRID)
        simplified
    }

}
