package com.databricks.labs.mosaic.core.index

import scala.annotation.tailrec
import scala.util.{Success, Try}

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.POLYGON
import com.databricks.labs.mosaic.core.types.model.MosaicChip
import org.locationtech.jts.geom.Geometry

/**
  * Implements the [[IndexSystem]] via BNG java implementation.
  *
  * @see
  *   [[https://en.wikipedia.org/wiki/Ordnance_Survey_National_Grid]]
  */
object BNGIndexSystem extends IndexSystem with Serializable {

    val quadrants = Seq("", "SW", "NW", "SE", "NE")
    val allowedResolutions = Set(1, -1, 2, -2, 3, -3, 4, -4, 5, -5, 6, -6)
    val resolutionMap =
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
    val sizeMap =
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
    val letterMap =
        Seq(
          Seq("SV", "SW", "SX", "SY", "SZ", "TV", "TW"),
          Seq("SQ", "SR", "SS", "ST", "SU", "TQ", "TR"),
          Seq("SL", "SM", "SN", "SO", "SP", "TL", "TM"),
          Seq("SF", "SG", "SH", "SJ", "SK", "TF", "TG"),
          Seq("SA", "SB", "SC", "SD", "SE", "TA", "TB"),
          Seq("NV", "NW", "NX", "NY", "NZ", "OV", "OW"),
          Seq("NQ", "NR", "NS", "NT", "NU", "OQ", "OR"),
          Seq("NL", "NM", "NN", "NO", "NP", "OL", "OM"),
          Seq("NF", "NG", "NH", "NJ", "NK", "OF", "OG"),
          Seq("NA", "NB", "NC", "ND", "NE", "OA", "OB"),
          Seq("HV", "HW", "HX", "HY", "SZ", "TV", "TW"),
          Seq("HQ", "HR", "HS", "HT", "HU", "JQ", "JR"),
          Seq("HL", "HM", "HN", "HO", "HP", "JL", "JM")
        )

    def format(index: Long): String = {
        val digits = indexDigits(index)
        if (digits.length < 6) {
            val prefix = letterMap(digits.slice(3, 5).mkString.toInt)(digits.slice(1, 3).mkString.toInt)(0).toString
            prefix
        } else {
            val quadrant = digits(5)
            val prefix = letterMap(digits.slice(3, 5).mkString.toInt)(digits.slice(1, 3).mkString.toInt)
            val coords = digits.drop(6)
            val k = coords.length / 2
            val xStr = if (coords.isEmpty) "" else coords.slice(0, k).mkString
            val yStr = if (coords.isEmpty) "" else coords.slice(k, 2 * k).mkString
            val qStr = quadrants(quadrant)
            s"$prefix$xStr$yStr$qStr"
        }
    }

    def indexDigits(index: Long): Seq[Int] = {
        index.toString.map(_.asDigit)
    }

    /**
      * A radius of minimal enclosing circle is always smaller than the largest
      * side of the skewed hexagon. Since H3 is generating hexagons that take
      * into account curvature of the spherical envelope a radius may be
      * different at different localities due to the skew. To address this
      * problem a centroid hexagon is selected from the geometry and the optimal
      * radius is computed based on this hexagon.
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
        val size = (10 ^ 6 - 10 ^ math.abs(resolution)) * (5 * 10 ^ (1 - math.abs(resolution)))
        size * math.sqrt(2)
    }

    /**
      * H3 polyfill logic is based on the centroid point of the individual index
      * geometry. Blind spots do occur near the boundary of the geometry.
      *
      * @param geometry
      *   Input geometry to be represented.
      * @param resolution
      *   A resolution of the indices.
      * @return
      *   A set of indices representing the input geometry.
      */
    override def polyfill(geometry: MosaicGeometry, resolution: Int, geometryAPI: Option[GeometryAPI]): Seq[Long] = {
        require(geometryAPI.isDefined, "GeometryAPI cannot be None for BNG Index System.")
        @tailrec
        def visit(queue: Set[Long], visited: Set[Long], result: Set[Long]): Set[Long] = {
            val visits = queue.map(index => (index, geometry.intersects(indexToGeometry(index, geometryAPI.get))))
            val matches = visits.filter(_._2)
            val newVisited = visited ++ visits.map(_._1)
            val newQueue = matches.flatMap(c => kDisk(c._1, 1).filterNot(newVisited.contains))
            val newResult = result ++ matches.map(_._1)
            if (queue.isEmpty) {
                newResult
            } else {
                visit(newQueue, newVisited, newResult)
            }
        }

        if (geometry.isEmpty) Seq.empty[Long]
        else {
            val shells = geometry.getShells
            val startPoints = shells.map(_.asSeq.head)
            val startIndices = startPoints.map(p => pointToIndex(p.getX, p.getY, resolution))
            visit(startIndices.toSet, Set.empty[Long], Set.empty[Long]).toSeq
        }
    }

    /**
      * Boundary that is returned by H3 isn't valid from JTS perspective since
      * it does not form a LinearRing (ie first point == last point). The first
      * point of the boundary is appended to the end of the boundary to form a
      * LinearRing.
      *
      * @param index
      *   Id of the index whose geometry should be returned.
      * @return
      *   An instance of [[Geometry]] corresponding to index.
      */
    override def indexToGeometry(index: Long, geometryAPI: GeometryAPI): MosaicGeometry = {
        val digits = indexDigits(index)
        val resolution = getResolution(digits)
        val edgeSize = getEdgeSize(digits, resolution)
        val x = getX(digits, edgeSize)
        val y = getY(digits, edgeSize)
        val p1 = geometryAPI.fromCoords(Seq(x, y))
        val p2 = geometryAPI.fromCoords(Seq(x + edgeSize, y))
        val p3 = geometryAPI.fromCoords(Seq(x + edgeSize, y + edgeSize))
        val p4 = geometryAPI.fromCoords(Seq(x, y + edgeSize))
        geometryAPI.geometry(Seq(p1, p2, p3, p4, p1), POLYGON)
    }

    def getX(digits: Seq[Int], edgeSize: Int): Int = {
        val n = digits.length
        val k = (n - 6) / 2
        val xDigits = digits.slice(1, 3) ++ digits.slice(6, 6 + k)
        xDigits.mkString.toInt * edgeSize
    }

    def getY(digits: Seq[Int], edgeSize: Int): Int = {
        val n = digits.length
        val k = (n - 6) / 2
        val yDigits = digits.slice(3, 5) ++ digits.slice(6 + k, 6 + 2 * k)
        yDigits.mkString.toInt * edgeSize
    }

    def getResolution(digits: Seq[Int]): Int = {
        if (digits.length < 6) {
            -1 // 500km resolution
        } else {
            val quadrant = digits(5)
            val n = digits.length
            val k = (n - 5) / 2
            if (quadrant > 0) {
                -(k + 1)
            } else {
                k + 1
            }
        }
    }

    def getEdgeSize(digits: Seq[Int], resolution: Int): Int = {
        if (digits.length < 6) {
            500000 // 500km resolution
        } else {
            val q = digits(5)
            val multiplier =
                if (q > 0) { 5 }
                else { 1 }
            val edgeSize = multiplier * math.pow(10, 6 - resolution)
            edgeSize.toInt
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
    override def kDisk(index: Long, k: Int): Seq[Long] = {
        val digits = indexDigits(index)
        val resolution = getResolution(digits)
        val edgeSize = getEdgeSize(digits, resolution)
        val x = getX(digits, edgeSize)
        val y = getY(digits, edgeSize)
        val bottom = (0 until 2 * k).map(c => (x + (c - k) * edgeSize, y - k * edgeSize))
        val right = (0 until 2 * k).map(c => (x + k * edgeSize, y + (c - k) * edgeSize))
        val top = (0 until 2 * k).map(c => (x + (k - c) * edgeSize, y + k * edgeSize))
        val left = (0 until 2 * k).map(c => (x - k * edgeSize, y + (k - c) * edgeSize))
        (bottom ++ right ++ top ++ left).map { case (x, y) => pointToIndex(x, y, resolution) }.filter(BNGIndexSystem.isValid)
    }

    def isValid(index: Long): Boolean = {
        val digits = indexDigits(index)
        val resolution = getResolution(digits)
        val edgeSize = getEdgeSize(digits, resolution)
        val x = getX(digits, edgeSize)
        val y = getY(digits, edgeSize)
        x >= 0 && x <= 700000 && y >= 0 && y <= 1300000
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
        val eLetter: Int = math.floor(eastings / 100000).toInt
        val nLetter: Int = math.floor(northings / 100000).toInt

        val offset: Int = if (resolution < -1) 1 else 0
        val divisor: Double = if (resolution < 0) math.pow(10, 6 - math.abs(resolution) + 1) else math.pow(10, 6 - resolution)
        val quadrant: Int = getQuadrant(resolution, eastings, northings, divisor)
        val nPositions: Int = math.abs(resolution) - offset

        val eBin: Int = math.floor((eastings % 100000) / divisor).toInt
        val nBin: Int = math.floor((northings % 100000) / divisor).toInt

        val idPlaceholder = math.pow(10, 5 + 2 * nPositions - 2) // 1(##)(##)(#)(#...#)(#...#)
        val eLetterShift = math.pow(10, 3 + 2 * nPositions - 2) // (##)(##)(#)(#...#)(#...#)
        val nLetterShift = math.pow(10, 1 + 2 * nPositions - 2) // (##)(#)(#...#)(#...#)
        val quadrantShift = math.pow(10, 2 * nPositions - 2) // (#)(#...#)(#...#)
        val eShift = math.pow(10, nPositions - 1) // (#...#)(#...#)
        val id =
            if (resolution == -1) {
                (idPlaceholder + eLetter * eLetterShift) / 100 + quadrant
            } else {
                idPlaceholder + eLetter * eLetterShift + nLetter * nLetterShift + quadrant * quadrantShift + eBin * eShift + nBin
            }
        id.toLong
    }

    def getQuadrant(resolution: Int, eastings: Double, northings: Double, divisor: Double): Int = {
        val quadrant: Int = {
            if (resolution < 0) {
                val eQ = eastings / divisor
                val nQ = northings / divisor
                val eDecimal = eQ - math.floor(eQ)
                val nDecimal = nQ - math.floor(nQ)
                (eDecimal, nDecimal) match {
                    case (e, n) if e < 0.5 & n < 0.5 => 1 // SW
                    case (e, _) if e < 0.5           => 2 // NW
                    case (_, n) if n < 0.5           => 3 // SE
                    case _                           => 4 // NE
                }
            } else 0
        }
        quadrant
    }

    /**
      * @see
      *   [[IndexSystem.getCoreChips()]]
      * @param coreIndices
      *   Indices corresponding to the core area of the input geometry.
      * @return
      *   A core area representation via [[MosaicChip]] set.
      */
    override def getCoreChips(coreIndices: Seq[Long], keepCoreGeom: Boolean, geometryAPI: GeometryAPI): Seq[MosaicChip] = {
        coreIndices.map(index => {
            val indexGeom = if (keepCoreGeom) indexToGeometry(index, geometryAPI) else null
            MosaicChip(isCore = true, index, indexGeom)
        })
    }

    /**
      * Returns the index system ID instance that uniquely identifies an index
      * system. This instance is used to select appropriate Mosaic expressions.
      *
      * @return
      *   An instance of [[IndexSystemID]]
      */
    override def getIndexSystemID: IndexSystemID = BNG

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
            Seq(index) ++ kDisk(index, 1)
        } else {
            Seq(index) ++ (1 to n).flatMap(kDisk(index, _))
        }
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
        Try({
            val resolution = res.asInstanceOf[Int]
            allowedResolutions.contains(res.asInstanceOf[Int])
            resolution
        }) match {
            case Success(value) => value
            case _              => Try({
                    val resolutionName = res.asInstanceOf[String]
                    val resolution = resolutionMap(resolutionName)
                    resolution
                }) match {
                    case Success(value) => value
                    case _              => throw new IllegalStateException(s"BNG resolution not supported; found $res")
                }
        }
    }

    override def minResolution: Int = -6

    override def maxResolution: Int = 6

}
