package com.databricks.labs.mosaic.core.index

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.types.model.Coordinates
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.POLYGON
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.Geometry

import scala.annotation.tailrec
import scala.util.{Success, Try}

/**
  * Implements the [[IndexSystem]] via BNG (British National Grid) java
  * implementation. BNG index system covers the EPSG:27700 bounds. The index
  * system is represented as a square grid, where x and y coordinates are
  * provided as eastings and northings. The index system supports representation
  * of index ids as integers and as strings. The index system supports providing
  * resolutions as integer numbers and as as string cell size descriptors (eg.
  * 500m for resolution where cell edge is 500 meters long). Negative resolution
  * values represent resolutions for quad tree representations where each cell
  * is split into orientation quadrants. Orientation quadrants represent
  * south-east, north-east, south-west and north-west orientations.
  *
  * @see
  *   [[https://en.wikipedia.org/wiki/Ordnance_Survey_National_Grid]]
  */
//noinspection ScalaWeakerAccess
object BNGIndexSystem extends IndexSystem(StringType) with Serializable {

    override def crsID: Int = 27700

    val name = "BNG"

    /**
      * Quadrant encodings. The order is determined in a way that preserves
      * similarity to space filling curves.
      */
    val quadrants: Seq[String] = Seq("", "SW", "NW", "NE", "SE")

    /**
      * Resolution mappings from string names to integer encodings. Resolutions
      * are uses as integers in any index math so we need to convert sizes to
      * corresponding index resolutions.
      */
    val resolutionMap: Map[String, Int] =
        Map(
          "500km" -> -1,
          "100km" -> 1,
          "50km" -> -2,
          "10km" -> 2,
          "5km" -> -3,
          "1km" -> 3,
          "500m" -> -4,
          "100m" -> 4,
          "50m" -> -5,
          "10m" -> 5,
          "5m" -> -6,
          "1m" -> 6
        )

    /**
      * Mapping from string names to edge sizes expressed in eastings/northings.
      */
    val sizeMap: Map[String, Int] =
        Map(
          "500km" -> 500000,
          "100km" -> 100000,
          "50km" -> 50000,
          "10km" -> 10000,
          "5km" -> 5000,
          "1km" -> 1000,
          "500m" -> 500,
          "100m" -> 100,
          "50m" -> 50,
          "10m" -> 10,
          "5m" -> 5,
          "1m" -> 1
        )

    /**
      * Matrix representing a mapping between letter portions of the eastings
      * and northings coordinates to a letter pair. Given th small area of
      * coverage of this index system having a lookup is more efficient than
      * performing any math transformations between ints and chars.
      */
    val letterMap: Seq[Seq[String]] =
        Seq(
          Seq("SV", "SW", "SX", "SY", "SZ", "TV", "TW", "TX"),
          Seq("SQ", "SR", "SS", "ST", "SU", "TQ", "TR", "TS"),
          Seq("SL", "SM", "SN", "SO", "SP", "TL", "TM", "TN"),
          Seq("SF", "SG", "SH", "SJ", "SK", "TF", "TG", "TH"),
          Seq("SA", "SB", "SC", "SD", "SE", "TA", "TB", "TC"),
          Seq("NV", "NW", "NX", "NY", "NZ", "OV", "OW", "OX"),
          Seq("NQ", "NR", "NS", "NT", "NU", "OQ", "OR", "OS"),
          Seq("NL", "NM", "NN", "NO", "NP", "OL", "OM", "ON"),
          Seq("NF", "NG", "NH", "NJ", "NK", "OF", "OG", "OH"),
          Seq("NA", "NB", "NC", "ND", "NE", "OA", "OB", "OC"),
          Seq("HV", "HW", "HX", "HY", "HZ", "JV", "JW", "JX"),
          Seq("HQ", "HR", "HS", "HT", "HU", "JQ", "JR", "JS"),
          Seq("HL", "HM", "HN", "HO", "HP", "JL", "JM", "JN"),
          Seq("HF", "HG", "HH", "HJ", "HK", "JF", "JG", "JH")
        )

    /**
      * Provides a string representation from an integer representation of a BNG
      * index id. The string representations follows letter prefix followed by
      * easting bin, followed by nothings bin and finally (for quad tree
      * resolutions) followed by quadrant suffix.
      * @param id
      *   Integer id to be formatted.
      * @return
      *   A string representation of the index id -
      *   "(prefix)(estings_bin)(northins_bin)(suffix)". E.g. SW123987NW where
      *   SW is the prefix, 123 is eastings bin, 987 is northings bin and NW is
      *   suffix.
      */
    override def format(id: Long): String = {
        val digits = indexDigits(id)
        if (digits.length < 6) {
            val prefix = letterMap(digits.slice(3, 5).mkString.toInt)(digits.slice(1, 3).mkString.toInt)(0).toString
            prefix
        } else {
            val quadrant = digits.last
            val prefix = letterMap(digits.slice(3, 5).mkString.toInt)(digits.slice(1, 3).mkString.toInt)
            val coords = digits.drop(5).dropRight(1)
            val k = coords.length / 2
            val xStr = if (coords.isEmpty) "" else coords.slice(0, k).padTo(k, 0).mkString
            val yStr = if (coords.isEmpty) "" else coords.slice(k, 2 * k).padTo(k, 0).mkString
            val qStr = quadrants(quadrant)
            s"$prefix$xStr$yStr$qStr"
        }
    }

    /**
      * Returns a half diagonal of the index geometry. Since this is a planar
      * index system, there is no need to account for skew, both diagonals have
      * the same length. It is sufficient to do square root of 2 times the
      * length of the edge to determine the diagonal.
      *
      * @param geometry
      *   An instance of [[MosaicGeometry]] for which we are computing the
      *   optimal buffer radius.
      * @param resolution
      *   A resolution to be used to get the centroid index geometry.
      * @return
      *   An optimal radius to buffer the geometry in order to avoid blind spots
      *   when performing polyfill.
      */
    override def getBufferRadius(geometry: MosaicGeometry, resolution: Int, geometryAPI: GeometryAPI): Double = {
        val size = getEdgeSize(resolution)
        size * math.sqrt(2) / 2
    }

    /**
      * Returns edge size for a given index resolution.
      * @param resolution
      *   Resolution at which we need to compute the edge size.
      * @return
      *   Edge size for the given resolution.
      */
    def getEdgeSize(resolution: Int): Int = {
        val resolutionStr = getResolutionStr(resolution)
        getEdgeSize(resolutionStr)
    }

    def getEdgeSize(resolution: String): Int = {
        sizeMap(resolution)
    }

    /**
      * Polyfill logic is based on the centroid point of the individual index
      * geometry. Blind spots do occur near the boundary of the geometry. The
      * decision to use centroid based logic is made to align with what is done
      * in H3 and unify the logic between index systems.
      *
      * @param geometry
      *   Input geometry to be represented.
      * @param resolution
      *   A resolution of the indices.
      * @return
      *   A set of indices representing the input geometry.
      */
    override def polyfill(geometry: MosaicGeometry, resolution: Int, geometryAPI: GeometryAPI): Seq[Long] = {
//        require(geometryAPI.isDefined, "GeometryAPI cannot be None for BNG Index System.")
        @tailrec
        def visit(queue: Set[Long], visited: Set[Long], result: Set[Long]): Set[Long] = {
            val visits = queue.map(index => (index, geometry.contains(indexToGeometry(index, geometryAPI).getCentroid)))
            val matches = visits.filter(_._2)
            val newVisited = visited ++ visits.map(_._1)
            val newQueue = matches.flatMap(c => kLoop(c._1, 1).filterNot(newVisited.contains))
            val newResult = result ++ matches.map(_._1)
            if (newQueue.isEmpty) {
                newResult
            } else {
                visit(newQueue, newVisited, newResult)
            }
        }

        if (geometry.isEmpty) Seq.empty[Long]
        else {
            val shells = geometry.getShells
            val holes = geometry.getHoles
            val startPoints = shells.flatMap(_.asSeq) ++ holes.flatMap(_.flatMap(_.asSeq)) ++ Seq(geometry.getCentroid)
            val startIndices = startPoints.map(p => pointToIndex(p.getX, p.getY, resolution))
            visit(startIndices.toSet, Set.empty[Long], Set.empty[Long]).toSeq
        }
    }

    /**
      * Get the k ring of indices around the provided index id.
      *
      * @param index
      *   Index ID to be used as a center of k ring.
      * @param n
      *   Number of k rings to be generated around the input index.
      * @return
      *   A collection of index IDs forming a k ring.
      */
    override def kRing(index: Long, n: Int): Seq[Long] = {
        if (n == 1) {
            Seq(index) ++ kLoop(index, 1)
        } else {
            Seq(index) ++ (1 to n).flatMap(kLoop(index, _))
        }
    }

    /**
      * Get the k disk of indices around the provided index id.
      *
      * @param index
      *   Index ID to be used as a center of k disk.
      * @param k
      *   Distance of k disk to be generated around the input index.
      * @return
      *   A collection of index IDs forming a k disk.
      */
    override def kLoop(index: Long, k: Int): Seq[Long] = {
        val digits = indexDigits(index)
        val resolution = getResolution(digits)
        val edgeSize = getEdgeSize(resolution)
        val x = getX(digits, edgeSize)
        val y = getY(digits, edgeSize)
        val bottom = (0 until 2 * k).map(c => (x + (c - k) * edgeSize, y - k * edgeSize))
        val right = (0 until 2 * k).map(c => (x + k * edgeSize, y + (c - k) * edgeSize))
        val top = (0 until 2 * k).map(c => (x + (k - c) * edgeSize, y + k * edgeSize))
        val left = (0 until 2 * k).map(c => (x - k * edgeSize, y + (k - c) * edgeSize))
        val neighbours = (bottom ++ right ++ top ++ left).map { case (x, y) => pointToIndex(x, y, resolution) }
        val result = neighbours.filter(BNGIndexSystem.isValid)
        result
    }

    /**
      * Checks if the provided index is within bounds of the index system.
      * @param index
      *   Index id to be checked.
      * @return
      *   Boolean representing validity.
      */
    override def isValid(index: Long): Boolean = {
        val digits = indexDigits(index)
        val xLetterIndex = digits.slice(3, 5).mkString.toInt
        val yLetterIndex = digits.slice(1, 3).mkString.toInt
        val resolution = getResolution(digits)
        val edgeSize = getEdgeSize(resolution)
        val x = getX(digits, edgeSize)
        val y = getY(digits, edgeSize)
        x >= 0 && x <= 700000 && y >= 0 && y <= 1300000 && xLetterIndex < letterMap.length && yLetterIndex < letterMap.head.length
    }

    /**
      * Get the index ID corresponding to the provided coordinates.
      *
      * @param eastings
      *   Eastings coordinate of the point.
      * @param northings
      *   Northings coordinate of the point.
      * @param resolution
      *   Resolution of the index.
      * @return
      *   Index ID in this index system.
      */
    override def pointToIndex(eastings: Double, northings: Double, resolution: Int): Long = {
        require(!eastings.isNaN && !northings.isNaN, throw new IllegalStateException("NaN coordinates are not supported."))
        val eastingsInt = eastings.toInt
        val northingsInt = northings.toInt
        val eLetter: Int = math.floor(eastingsInt / 100000).toInt
        val nLetter: Int = math.floor(northingsInt / 100000).toInt

        val divisor: Double = if (resolution < 0) math.pow(10, 6 - math.abs(resolution) + 1) else math.pow(10, 6 - resolution)
        val quadrant: Int = getQuadrant(resolution, eastingsInt, northingsInt, divisor)
        val nPositions: Int = if (resolution >= -1) math.abs(resolution) else math.abs(resolution) - 1

        val eBin: Int = math.floor((eastingsInt % 100000) / divisor).toInt
        val nBin: Int = math.floor((northingsInt % 100000) / divisor).toInt
        encode(eLetter, nLetter, eBin, nBin, quadrant, nPositions, resolution)
    }

    /**
      * Computes the quadrant based on the resolution, coordinates and a
      * divisor.
      * @param resolution
      *   Resolution of the index system.
      * @param eastings
      *   X coordinate of the point.
      * @param northings
      *   Y coordinate of the point.
      * @param divisor
      *   Divisor is equal to edge size for positive index resolutions and is
      *   equal to 2x of the edge size for negative index resolutions.
      * @return
      *   An integer representing the quadrant. 0 is reserved for resolutions
      *   that do not have quadrant representation.
      */
    def getQuadrant(resolution: Int, eastings: Double, northings: Double, divisor: Double): Int = {
        val quadrant: Int = {
            if (resolution < -1) {
                val eQ = eastings / divisor
                val nQ = northings / divisor
                val eDecimal = eQ - math.floor(eQ)
                val nDecimal = nQ - math.floor(nQ)
                (eDecimal, nDecimal) match {
                    // quadrant traversal SW->NW->NE->SE for
                    // better space-filling
                    case (e, n) if e < 0.5 & n < 0.5 => 1 // SW
                    case (e, _) if e < 0.5           => 2 // NW
                    case (_, n) if n < 0.5           => 4 // SE
                    case _                           => 3 // NE
                }
            } else 0
        }
        quadrant
    }

    /**
      * BNG resolution can only be an Int value between 0 and 6. Traditional
      * resolutions only support base 10 edge size of the index. In addition to
      * 0 to 6 resolution, there are mid way resolutions that split index into
      * quadrants. Those are denoted as .5 resolutions by convention.
      *
      * @see
      *   [[IndexSystem.getResolution()]] docs.
      * @param res
      *   Any type input to be parsed into the Int representation of resolution.
      * @return
      *   Int value representing the resolution.
      */
    override def getResolution(res: Any): Int = {
        (
          Try(res.asInstanceOf[Int]),
          Try(res.asInstanceOf[String]),
          Try(res.asInstanceOf[UTF8String].toString)
        ) match {
            case (Success(value), _, _) if resolutions.contains(value)   => value
            case (_, _, Success(value)) if resolutionMap.contains(value) => resolutionMap(value)
            case (_, Success(value), _) if resolutionMap.contains(value) => resolutionMap(value)
            case _ => throw new IllegalStateException(s"BNG resolution not supported; found $res")
        }
    }

    /**
      * Resolutions in BNG are split into positive and negative resolutions.
      * Positive resolutions represent grids which cells have lengths in base
      * 10. Negative resolution represent grids which cells have lengths in base
      * 50. Negative resolutions correspond to a quad tree inside the base 10
      * BNG grid where each cell is split into SouthEast, NorthEast, SouthWest
      * and NorthWest quadrants.
      * @return
      *   A set of supported resolutions.
      */
    override def resolutions: Set[Int] = Set(1, -1, 2, -2, 3, -3, 4, -4, 5, -5, 6, -6)

    /**
      * Provides a long representation from a string representation of a BNG
      * index id. The string representations follows letter prefix followed by
      * easting bin, followed by nothings bin and finally (for quad tree
      * resolutions) followed by quadrant suffix.
      * @param index
      *   String id to be parsed.
      * @return
      *   A long representation of the index id -
      *   "1(eastings_letter_encoding)(northings_letter_encoding)(eastings_bin)(northings_bin)(quadrants)".
      */
    def parse(index: String): Long = {
        val prefix = if (index.length >= 2) index.take(2) else s"${index}V"
        val letterRow = letterMap.find(_.contains(prefix)).get
        val eLetter: Int = letterRow.indexOf(prefix)
        val nLetter: Int = letterMap.indexOf(letterRow)

        if (index.length == 1) {
            encode(eLetter, 0, 0, 0, 0, 1, -1)
        } else {
            val suffix = index.slice(index.length - 2, index.length)
            val quadrant: Int = if (quadrants.contains(suffix) && index.length > 2) quadrants.indexOf(suffix) else 0
            val binDigits = if (quadrant > 0) index.drop(2).dropRight(2) else index.drop(2)
            if (binDigits.isEmpty) {
                encode(eLetter, nLetter, 0, 0, quadrant, 1, -2)
            } else {
                val eBin: Int = binDigits.dropRight(binDigits.length / 2).toInt
                val nBin: Int = binDigits.drop(binDigits.length / 2).toInt
                val nPositions: Int = binDigits.length / 2 + 1
                val resolution = if (quadrant == 0) nPositions + 1 else -nPositions
                encode(eLetter, nLetter, eBin, nBin, quadrant, nPositions, resolution)
            }
        }
    }

    /**
      * Constructs a geometry representing the index tile corresponding to
      * provided index id.
      *
      * @param index
      *   Id of the index whose geometry should be returned.
      * @return
      *   An instance of [[Geometry]] corresponding to index.
      */
    override def indexToGeometry(index: Long, geometryAPI: GeometryAPI): MosaicGeometry = {
        val digits = indexDigits(index)
        val resolution = getResolution(digits)
        val edgeSize = getEdgeSize(resolution)
        val x = getX(digits, edgeSize)
        val y = getY(digits, edgeSize)
        val p1 = geometryAPI.fromCoords(Seq(x, y))
        val p2 = geometryAPI.fromCoords(Seq(x + edgeSize, y))
        val p3 = geometryAPI.fromCoords(Seq(x + edgeSize, y + edgeSize))
        val p4 = geometryAPI.fromCoords(Seq(x, y + edgeSize))
        val geom = geometryAPI.geometry(Seq(p1, p2, p3, p4, p1), POLYGON)
        geom.setSpatialReference(this.crsID)
        geom
    }

    /**
      * Returns index as a sequence of digits.
      * @param index
      *   Index to be split into digits.
      * @return
      *   Index digits.
      */
    def indexDigits(index: Long): Seq[Int] = {
        index.toString.map(_.asDigit)
    }

    /**
      * Computes the resolution based on the index digits.
      * @param digits
      *   Index digits.
      * @return
      *   Resolution that results in this length of digits.
      */
    def getResolution(digits: Seq[Int]): Int = {
        if (digits.length < 6) {
            -1 // 500km resolution
        } else {
            val quadrant = digits.last
            val n = digits.length
            val k = (n - 6) / 2
            if (quadrant > 0) {
                -(k + 2)
            } else {
                k + 1
            }
        }
    }

    /**
      * X coordinate based on the digits of the index and the edge size. X
      * coordinate is rounded to the edge size precision. X coordinate
      * corresponds to eastings coordinate.
      * @param digits
      *   Index digits.
      * @param edgeSize
      *   Index edge size.
      * @return
      *   X coordinate.
      */
    def getX(digits: Seq[Int], edgeSize: Int): Int = {
        val n = digits.length
        val k = (n - 6) / 2
        val xDigits = digits.slice(1, 3) ++ digits.slice(5, 5 + k)
        val quadrant = digits.last
        val edgeSizeAdj = if (quadrant > 0) 2 * edgeSize else edgeSize
        val xOffset = if (quadrant == 3 || quadrant == 4) edgeSize else 0
        xDigits.mkString.toInt * edgeSizeAdj + xOffset
    }

    /**
      * Y coordinate based on the digits of the index and the edge size. Y
      * coordinate is rounded to the edge size precision. Y coordinate
      * corresponds to northings coordinate.
      * @param digits
      *   Index digits.
      * @param edgeSize
      *   Index edge size.
      * @return
      *   Y coordinate.
      */
    def getY(digits: Seq[Int], edgeSize: Int): Int = {
        val n = digits.length
        val k = (n - 6) / 2
        val yDigits = digits.slice(3, 5) ++ digits.slice(5 + k, 5 + 2 * k)
        val quadrant = digits.last
        val edgeSizeAdj = if (quadrant > 0) 2 * edgeSize else edgeSize
        val yOffset = if (quadrant == 2 || quadrant == 3) edgeSize else 0
        yDigits.mkString.toInt * edgeSizeAdj + yOffset
    }

    override def getResolutionStr(resolution: Int): String = resolutionMap.find(_._2 == resolution).map(_._1).getOrElse("")

    override def area(index: Long): Double = {
        val digits = indexDigits(index)
        val resolution = getResolution(digits)
        val edgeSize = getEdgeSize(resolution).asInstanceOf[Double]
        val area = math.pow(edgeSize / 1000, 2)
        area
    }

    override def indexToCenter(index: Long): Coordinates = {
        throw new NotImplementedError
    }

    override def indexToBoundary(index: Long): Seq[Coordinates] = {
        throw new NotImplementedError
    }

    override def distance(cellId: Long, cellId2: Long): Long = {
        val digits1 = indexDigits(cellId)
        val digits2 = indexDigits(cellId2)
        val resolution1 = getResolution(digits1)
        val resolution2 = getResolution(digits2)
        val edgeSize = getEdgeSize(math.min(resolution1, resolution2))
        val x1 = getX(digits1, edgeSize)
        val x2 = getX(digits2, edgeSize)
        val y1 = getY(digits1, edgeSize)
        val y2 = getY(digits2, edgeSize)
        // Manhattan distance with edge size precision
        math.abs((x1 - x2) / edgeSize) + math.abs((y1 - y2) / edgeSize)
    }

    def encode(eLetter: Int, nLetter: Int, eBin: Int, nBin: Int, quadrant: Int, nPositions: Int, resolution: Int): Long = {
        val idPlaceholder = math.pow(10, 5 + 2 * nPositions - 2) // 1(##)(##)(#...#)(#...#)(#)
        val eLetterShift = math.pow(10, 3 + 2 * nPositions - 2) // (##)(##)(#...#)(#...#)(#)
        val nLetterShift = math.pow(10, 1 + 2 * nPositions - 2) // (##)(#...#)(#...#)(#)
        val eShift = math.pow(10, nPositions) // (#...#)(#...#)(#)
        val nShift = 10
        val id =
            if (resolution == -1) {
                (idPlaceholder + eLetter * eLetterShift) / 100 + quadrant
            } else {
                idPlaceholder + eLetter * eLetterShift + nLetter * nLetterShift + eBin * eShift + nBin * nShift + quadrant
            }
        id.toLong
    }

}
