package com.databricks.labs.gbx.gridx.grid

import com.databricks.labs.gbx.vectorx.jts.GeometryTypeEnum._
import com.databricks.labs.gbx.vectorx.jts.{GeometryTypeEnum, JTS}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.{Success, Try}

/**
  * Implements BNG (British National Grid) java implementation. BNG grid system
  * covers the EPSG:27700 bounds. The grid system is represented as a square
  * grid, where x and y coordinates are provided as eastings and northings. The
  * grid system supports representation of cell ids as integers and as strings.
  * The grid system supports providing resolutions as integer numbers and as
  * string cell size descriptors (eg. 500m for resolution where cell edge is 500
  * meters long). Negative resolution values represent resolutions for quad tree
  * representations where each cell is split into orientation quadrants.
  * Orientation quadrants represent south-east, north-east, south-west and
  * north-west orientations.
  *
  * @see
  *   [[https://en.wikipedia.org/wiki/Ordnance_Survey_National_Grid]]
  */
//noinspection ScalaWeakerAccess
object BNG extends Serializable {

    def cellType(idType: DataType): StructType =
        StructType(
          Array(
            StructField("cellid", idType, nullable = false),
            StructField("core", BooleanType, nullable = false),
            StructField("chip", BinaryType, nullable = false)
          )
        )

    def crsID: Int = 27700

    val name = "BNG"

    /**
      * Quadrant encodings. The order is determined in a way that preserves
      * similarity to space filling curves.
      */
    val quadrants: Seq[String] = Seq("", "SW", "NW", "NE", "SE")

    /**
      * Resolution mappings from string names to integer encodings. Resolutions
      * are uses as integers in any grid math so we need to convert sizes to
      * corresponding grid resolutions.
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
      * coverage of this grid system having a lookup is more efficient than
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
      * cell id. The string representations follows letter prefix followed by
      * easting bin, followed by nothings bin and finally (for quad tree
      * resolutions) followed by quadrant suffix.
      * @param id
      *   Integer id to be formatted.
      * @return
      *   A string representation of the cell id -
      *   "(prefix)(estings_bin)(northins_bin)(suffix)". E.g. SW123987NW where
      *   SW is the prefix, 123 is eastings bin, 987 is northings bin and NW is
      *   suffix.
      */
    def format(id: Long): String = {
        val digits = cellDigits(id)
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
      * Returns a half diagonal of the cell geometry. Since this is a planar
      * grid system, there is no need to account for skew, both diagonals have
      * the same length. It is sufficient to do square root of 2 times the
      * length of the edge to determine the diagonal.
      *
      * @param geometry
      *   An instance of [[Geometry]] for which we are computing the optimal
      *   buffer radius.
      * @param resolution
      *   A resolution to be used to get the centroid cell geometry.
      * @return
      *   An optimal radius to buffer the geometry in order to avoid blind spots
      *   when performing polyfill.
      */
    def getBufferRadius(geometry: Geometry, resolution: Int): Double = {
        val size = getEdgeSize(resolution)
        size * math.sqrt(2) / 2
    }

    /**
      * Returns edge size for a given grid resolution.
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
      * Polyfill logic is based on the centroid point of the individual cell
      * geometry. Blind spots do occur near the boundary of the geometry. The
      * decision to use centroid based logic is made to align with what is done
      * in H3 and unify the logic between grid systems.
      *
      * @param geometry
      *   Input geometry to be represented.
      * @param resolution
      *   A resolution of the indices.
      * @return
      *   A set of indices representing the input geometry.
      */
    def polyfill(geometry: Geometry, resolution: Int): Iterator[Long] = {
        if (geometry.isEmpty) return Iterator.empty

        val startPoints = geometry.getCoordinates ++ geometry.getCentroid.getCoordinates
        val startIndices = startPoints.map(p => pointToCellID(p.getX, p.getY, resolution))

        new Iterator[Long] {
            private val visited = mutable.HashSet.empty[Long]
            private val queue = mutable.Queue.empty[Long]
            queue ++= startIndices

            private var nextElem: Option[Long] = None

            private def advance(): Unit = {
                while (queue.nonEmpty && nextElem.isEmpty) {
                    val current = queue.dequeue()
                    if (!visited.contains(current)) {
                        visited += current
                        val center = cellIdToGeometry(current).getCentroid
                        if (geometry.contains(center)) {
                            nextElem = Some(current)
                            val neighbors = kLoop(current, 1).filterNot(visited.contains)
                            queue ++= neighbors
                        }
                    }
                }
            }

            override def hasNext: Boolean = {
                if (nextElem.isEmpty) advance()
                nextElem.isDefined
            }

            override def next(): Long = {
                if (!hasNext) throw new NoSuchElementException
                val result = nextElem.get
                nextElem = None
                result
            }
        }
    }

    /**
      * Get the k ring of indices around the provided cell id.
      *
      * @param cellID
      *   Cell ID to be used as a center of k ring.
      * @param n
      *   Number of k rings to be generated around the input cell ID.
      * @return
      *   A collection of cell IDs forming a k ring.
      */
    def kRing(cellID: Long, n: Int): Iterator[Long] = {
        if (n == 1) Iterator.single(cellID) ++ kLoop(cellID, 1)
        else Iterator.single(cellID) ++ (1 to n).iterator.flatMap(k => kLoop(cellID, k))
    }

    /**
      * Get the k loop / k disk of indices around the provided cell id.
      *
      * @param cellID
      *   Cell ID to be used as a center of k disk.
      * @param k
      *   Distance of k disk to be generated around the input cell ID.
      * @return
      *   A collection of cell IDs forming a k disk.
      */
    def kLoop(cellID: Long, k: Int): Iterator[Long] = {
        val digits = cellDigits(cellID)
        val resolution = getResolution(digits)
        val edgeSize = getEdgeSize(resolution)
        val x = getX(digits, edgeSize)
        val y = getY(digits, edgeSize)

        val xmin = x - k * edgeSize
        val xmax = x + k * edgeSize
        val ymin = y - k * edgeSize
        val ymax = y + k * edgeSize

        val corners = Iterator((xmin, ymin), (xmin, ymax), (xmax, ymax), (xmax, ymin))
        val left = (ymin + edgeSize until ymax by edgeSize).iterator.map(y => (xmin, y))
        val up = (xmin + edgeSize until xmax by edgeSize).iterator.map(x => (x, ymax))
        val right = (ymin + edgeSize until ymax by edgeSize).iterator.map(y => (xmax, y))
        val down = (xmin + edgeSize until xmax by edgeSize).iterator.map(x => (x, ymin))

        (corners ++ left ++ right ++ up ++ down).map { case (x, y) => pointToCellID(x, y, resolution) }
    }

    /**
      * Checks if the provided cell ID is within bounds of the grid system.
      * @param cellID
      *   Cell ID to be checked.
      * @return
      *   Boolean representing validity.
      */
    def isValid(cellID: Long): Boolean = {
        val digits = cellDigits(cellID)
        val xLetterIndex = digits.slice(3, 5).mkString.toInt
        val yLetterIndex = digits.slice(1, 3).mkString.toInt
        val resolution = getResolution(digits)
        val edgeSize = getEdgeSize(resolution)
        val x = getX(digits, edgeSize)
        val y = getY(digits, edgeSize)
        x >= 0 && x <= 700000 && y >= 0 && y <= 1300000 && xLetterIndex < letterMap.length && yLetterIndex < letterMap.head.length
    }

    /**
      * Get the cell ID corresponding to the provided coordinates.
      *
      * @param eastings
      *   Eastings coordinate of the point.
      * @param northings
      *   Northings coordinate of the point.
      * @param resolution
      *   Resolution of the grid.
      * @return
      *   Cell ID in this grid system.
      */
    def pointToCellID(eastings: Double, northings: Double, resolution: Int): Long = {
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
      *   Resolution of the grid system.
      * @param eastings
      *   X coordinate of the point.
      * @param northings
      *   Y coordinate of the point.
      * @param divisor
      *   Divisor is equal to edge size for positive grid resolutions and is
      *   equal to 2x of the edge size for negative grid resolutions.
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
      * resolutions only support base 10 edge size of the grid. In addition to 0
      * to 6 resolution, there are mid way resolutions that split cells into
      * quadrants. Those are denoted as .5 resolutions by convention.
      *
      * @param res
      *   Any type input to be parsed into the Int representation of resolution.
      * @return
      *   Int value representing the resolution.
      */
    def getResolution(res: Any): Int = {
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
    def resolutions: Set[Int] = Set(1, -1, 2, -2, 3, -3, 4, -4, 5, -5, 6, -6)

    /**
      * Provides a long representation from a string representation of a BNG
      * cell id. The string representations follows letter prefix followed by
      * easting bin, followed by nothings bin and finally (for quad tree
      * resolutions) followed by quadrant suffix.
      * @param cellID
      *   String id to be parsed.
      * @return
      *   A long representation of the cell id -
      *   "1(eastings_letter_encoding)(northings_letter_encoding)(eastings_bin)(northings_bin)(quadrants)".
      */
    def parse(cellID: String): Long = {
        val prefix = if (cellID.length >= 2) cellID.take(2) else s"${cellID}V"
        val letterRow = letterMap.find(_.contains(prefix)).get
        val eLetter: Int = letterRow.indexOf(prefix)
        val nLetter: Int = letterMap.indexOf(letterRow)

        if (cellID.length == 1) {
            encode(eLetter, 0, 0, 0, 0, 1, -1)
        } else {
            val suffix = cellID.slice(cellID.length - 2, cellID.length)
            val quadrant: Int = if (quadrants.contains(suffix) && cellID.length > 2) quadrants.indexOf(suffix) else 0
            val binDigits = if (quadrant > 0) cellID.drop(2).dropRight(2) else cellID.drop(2)
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
      * Constructs a geometry representing the grid tile corresponding to
      * provided cell id.
      *
      * @param cellID
      *   Id of the cell whose geometry should be returned.
      * @return
      *   An instance of [[Geometry]] corresponding to cell ID.
      */
    def cellIdToGeometry(cellID: Long): Geometry = {
        val digits = cellDigits(cellID)
        val resolution = getResolution(digits)
        val edgeSize = getEdgeSize(resolution)
        val x = getX(digits, edgeSize)
        val y = getY(digits, edgeSize)
        val geom = JTS.polygonFromXYs(Array((x, y), (x + edgeSize, y), (x + edgeSize, y + edgeSize), (x, y + edgeSize), (x, y)))
        geom.setSRID(this.crsID)
        geom
    }

    /**
      * Returns cell ID as a sequence of digits.
      * @param cellID
      *   Cell ID to be split into digits.
      * @return
      *   Cell ID digits.
      */
    def cellDigits(cellID: Long): Seq[Int] = {
        cellID.toString.map(_.asDigit)
    }

    /**
      * Computes the resolution based on the cell digits.
      * @param digits
      *   Cell ID digits.
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
      * X coordinate based on the digits of the cell id and the edge size. X
      * coordinate is rounded to the edge size precision. X coordinate
      * corresponds to eastings coordinate.
      * @param digits
      *   Cell ID digits.
      * @param edgeSize
      *   Cell edge size.
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
      * Y coordinate based on the digits of the cell ID and the edge size. Y
      * coordinate is rounded to the edge size precision. Y coordinate
      * corresponds to northings coordinate.
      * @param digits
      *   Cell ID digits.
      * @param edgeSize
      *   Cell edge size.
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

    def getResolutionStr(resolution: Int): String = resolutionMap.find(_._2 == resolution).map(_._1).getOrElse("")

    def area(cellID: Long): Double = {
        val digits = cellDigits(cellID)
        val resolution = getResolution(digits)
        val edgeSize = getEdgeSize(resolution).asInstanceOf[Double]
        val area = math.pow(edgeSize / 1000, 2)
        area
    }

    def cellIdToCenter(cellID: Long): Coordinate = {
        val geom = cellIdToGeometry(cellID)
        val centroid = geom.getCentroid
        JTS.coordinatesFromXYs(centroid.getX, centroid.getY)
    }

    def cellIdToBoundary(cellID: Long): Seq[Coordinate] = {
        val geom = cellIdToGeometry(cellID)
        val coordinates = geom.getCoordinates
        coordinates.map(coord => JTS.coordinatesFromXYs(coord.getX, coord.getY))
    }

    def distance(cellId: Long, cellId2: Long): Long = {
        val digits1 = cellDigits(cellId)
        val digits2 = cellDigits(cellId2)
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

    def euclideanDistance(cellId: Long, cellId2: Long): Long = {
        val digits1 = cellDigits(cellId)
        val digits2 = cellDigits(cellId2)
        val res1 = getResolution(digits1)
        val res2 = getResolution(digits2)
        val edgeSize = getEdgeSize(math.min(res1, res2))
        val x1 = getX(digits1, edgeSize)
        val x2 = getX(digits2, edgeSize)
        val y1 = getY(digits1, edgeSize)
        val y2 = getY(digits2, edgeSize)
        // euclidian distance with edge size precision
        // along diagonal the distance is 1, where manhattan distance would be 2
        math.max(math.abs(x1 - x2), math.abs(y1 - y2)) / edgeSize
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

    def geometryKLoop(geometry: Geometry, resolution: Int, k: Int): Set[Long] = {
        // TODO: MOVE TO ITERATOR
        val n: Int = k - 1
        // This has to be converted from iterator to a Seq, as we need to know what should be excluded
        // anything that was core will never be a part of a k-loop
        val chips = getChips(geometry, resolution, keepCoreGeom = false).toSeq
        val (coreCells, borderCells) = chips.partition(_._2)
        val coreIDs = coreCells.map(_._1).toSet

        // We use nRing as naming for kRing where k = n
        val borderNRing = borderCells.flatMap(c => kRing(c._1, n))
        val nRing = coreIDs ++ borderNRing

        val borderKLoop = borderCells.toSet.flatMap((c: (Long, Boolean, Geometry)) => this.kLoop(c._1, k))

        val kLoop = borderKLoop.diff(nRing)
        kLoop.filter(BNG.isValid)
    }

    def geometryKRing(geometry: Geometry, resolution: Int, k: Int): Set[Long] = {
        // TODO: MOVE TO ITERATOR
        val chips = getChips(geometry, resolution, keepCoreGeom = false).toSeq
        val (coreCells, borderCells) = chips.partition(_._2)
        val coreIDs = coreCells.map(_._1).toSet
        val borderKRing = borderCells.flatMap(c => kRing(c._1, k))
        (coreIDs ++ borderKRing).filter(BNG.isValid)
    }

    def getChips(
        geometry: Geometry,
        resolution: Int,
        keepCoreGeom: Boolean
    ): Iterator[(Long, Boolean, Geometry)] = {
        GeometryTypeEnum(geometry.getGeometryType) match {
            case POINT           => pointChip(geometry, resolution, keepCoreGeom)
            case MULTIPOINT      => multiPointChips(geometry, resolution, keepCoreGeom)
            case LINESTRING      => lineFill(geometry, resolution)
            case MULTILINESTRING => lineFill(geometry, resolution)
            case _               => tessellate(geometry, resolution, keepCoreGeom)
        }
    }

    def pointChip(
        geometry: Geometry,
        resolution: Int,
        keepCoreGeom: Boolean
    ): Iterator[(Long, Boolean, Geometry)] = {
        val point = geometry.asInstanceOf[Point]
        val chipGeom = if (keepCoreGeom) point else null
        val cellId = pointToCellID(point.getX, point.getY, resolution)
        Iterator.single((cellId, false, chipGeom))
    }

    def multiPointChips(
        geometry: Geometry,
        resolution: Int,
        keepCoreGeom: Boolean
    ): Iterator[(Long, Boolean, Geometry)] = {
        val n = geometry.getNumGeometries
        (0 until n).iterator.flatMap { i => pointChip(geometry.getGeometryN(i), resolution, keepCoreGeom) }
    }

    def lineFill(geometry: Geometry, resolution: Int): Iterator[(Long, Boolean, Geometry)] = {
        GeometryTypeEnum(geometry.getGeometryType) match {
            case LINESTRING      => lineDecompose(geometry.asInstanceOf[LineString], resolution)
            case MULTILINESTRING =>
                val multiLine = geometry.asInstanceOf[MultiLineString]
                val lines = (0 until multiLine.getNumGeometries).iterator.map(multiLine.getGeometryN)
                lines.flatMap(line => lineDecompose(line.asInstanceOf[LineString], resolution))
            case gt              => throw new Error(s"$gt not supported for line fill/decompose operation.")
        }
    }

    // TODO: This should be possible to optimize by better handling of the
    //      intersection of the line with the cell geometry.
    // perhaps queue logic can be replaced with a more efficient way of selecting
    // what to process next.
    private def lineDecompose(
        line: LineString,
        resolution: Int
    ): Iterator[(Long, Boolean, Geometry)] = {
        val start = line.getStartPoint
        val startCellID = pointToCellID(start.getX, start.getY, resolution)

        @tailrec
        def traverseLine(
            line: LineString,
            queue: Iterator[Long],
            traversed: Set[Long],
            chips: Iterator[(Long, Boolean, Geometry)]
        ): Iterator[(Long, Boolean, Geometry)] = {
            val newTraversed = traversed ++ queue
            val (newQueue, newChips) = queue.foldLeft(
              (Iterator.empty[Long], chips)
            )((accumulator: (Iterator[Long], Iterator[(Long, Boolean, Geometry)]), current: Long) => {
                val cellGeom = cellIdToGeometry(current)
                val lineSegment = line.intersection(cellGeom)
                if (!lineSegment.isEmpty) {
                    val chip = (current, false, lineSegment)
                    val kRing = this.kRing(current, 1)

                    // Ignore already processed chips and those which are already in the
                    // queue to be processed
                    val toQueue = kRing.filterNot((newTraversed ++ accumulator._1).contains)
                    (accumulator._1 ++ toQueue, accumulator._2 ++ Iterator.single(chip))
                } else if (newTraversed.size == 1) {
                    // The line segment intersection was empty, but we only intersected the first point
                    // with a single cell.
                    // We need to run an intersection with a first ring because the starting point might be laying
                    // exactly on the cell boundary.
                    val kRing = this.kRing(current, 1)
                    // In theory getting the next from Iterator that is not empty should be safe, but we
                    // will enqueue all the kRing cells anyway.
                    val toQueue = kRing.filterNot(newTraversed.contains)
                    (toQueue, accumulator._2)
                } else {
                    accumulator
                }
            })
            if (newQueue.isEmpty) {
                newChips
            } else {
                traverseLine(line, newQueue, newTraversed, newChips)
            }
        }

        val result = traverseLine(line, Iterator.single(startCellID), Set.empty[Long], Iterator.empty[(Long, Boolean, Geometry)])
        result
    }

    // TODO: This needs some optimization, as now it is moved to BNG and we can
    //      avoid generic logic that was introducing some overhead.
    def tessellate(
        geometry: Geometry,
        resolution: Int,
        keepCoreGeom: Boolean
    ): Iterator[(Long, Boolean, Geometry)] = {

        val inGeomType = geometry.getGeometryType
        val radius = getBufferRadius(geometry, resolution)
        val carvedGeometry = geometry.buffer(-radius)

        // add 1% to the radius to ensure union of carved and border geometries does not have holes inside the original geometry areas
        val borderGeometry =
            if (carvedGeometry.isEmpty) {
                JTS.simplify(
                  geometry.buffer(radius * 1.01),
                  0.01 * radius
                )
            } else {
                JTS.simplify(
                  geometry.getBoundary.buffer(radius * 1.01),
                  0.01 * radius
                )
            }

        // check that the resulting geometry is within the bounds of
        // the coordinate system (otherwise behaviour will be unpredictable)

        val coreIndices = polyfill(carvedGeometry, resolution)
        val coreSet = coreIndices.toSet
        val borderIndices = polyfill(borderGeometry, resolution).filterNot(coreSet.contains)

        val coreChips = coreSet.iterator.map(cell => {
            val coreGeom = if (keepCoreGeom) cellIdToGeometry(cell) else null
            (cell, true, coreGeom)
        })
        val borderChips = borderIndices.map(cell => {
            val cellGeom = cellIdToGeometry(cell)
            val intersect = cellGeom.intersection(geometry)
            if (intersect.isEmpty) (cell, false, intersect)
            else {
                val adjusted =
                    if (GeometryTypeEnum(intersect.getGeometryType) == GEOMETRYCOLLECTION) {
                        intersect.difference(cellGeom.getBoundary)
                    } else {
                        intersect
                    }
                // Tolerance is set to 0.1 to account for floating point precision issues
                // BNG coordinates are integers, so we can use a relatively large tolerance
                // as coordinates will be in meters and tolerance is a fraction of a meter.
                val isCore = adjusted.equalsExact(cellGeom, 0.1)
                if (isCore) {
                    if (keepCoreGeom) (cell, true, cellGeom)
                    else (cell, true, null)
                } else {
                    (cell, false, adjusted)
                }
            }
        }).filterNot(_._3.isEmpty)

        (coreChips ++ borderChips).filter(t => t._3 == null || t._3.getGeometryType == inGeomType)
    }

}
