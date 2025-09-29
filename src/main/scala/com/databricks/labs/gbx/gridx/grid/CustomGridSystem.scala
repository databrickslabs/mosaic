package com.databricks.labs.gbx.gridx.grid


//
//import JTS
//import org.apache.spark.unsafe.types.UTF8String
//import org.locationtech.jts.geom.{Coordinate, Geometry}
//
//import scala.util.{Success, Try}
//
////noinspection ScalaWeakerAccess
//case class CustomGridSystem(conf: GridConf) extends Serializable {
//
//    def crsID: Int =
//        conf.crsID.getOrElse(
//          throw new Error("CRS ID is not defined for this grid system")
//        )
//
//    val name =
//        f"CUSTOM(${conf.boundXMin}, ${conf.boundXMax}, ${conf.boundYMin}, ${conf.boundYMax}, ${conf.cellSplits}, ${conf.rootCellSizeX}, ${conf.rootCellSizeY})"
//
//    def getResolutionStr(resolution: Int): String = resolution.toString
//
//    def format(id: Long): String = id.toString
//
//    def parse(id: String): Long = id.toLong
//
//    /**
//      * Get the k ring of indices around the provided cell id.
//      *
//      * @param cellID
//      *   Cell ID to be used as a center of k ring.
//      * @param k
//      *   Number of k rings to be generated around the input cell ID.
//      * @return
//      *   A collection of cell IDs forming a k ring.
//      */
//
//    def kRing(cellID: Long, k: Int): Seq[Long] = {
//        assert(k >= 0, "k must be at least 0")
//
//        val res = getCellResolution(cellID)
//
//        val cellPosition = getCellPosition(cellID: Long)
//        val posX = getCellPositionX(cellPosition, res)
//        val posY = getCellPositionY(cellPosition, res)
//
//        val fromX = math.max(posX - k, 0)
//        val toX = math.min(posX + k, totalCellsX(res))
//
//        val fromY = math.max(posY - k, 0)
//        val toY = math.min(posY + k, totalCellsY(res))
//
//        (fromX to toX)
//            // Get all cells that overlap with the bounding box
//            .flatMap(x => (fromY to toY).map(y => (x, y)))
//
//            // Map them to cell centers and cell ID
//            .map(pos => getCellPositionFromPositions(pos._1, pos._2, res))
//            .map(pos => getCellId(pos, res))
//    }
//
//    /**
//      * Get the k loop (hollow ring) of indices around the provided cell id.
//      *
//      * @param cellID
//      *   Cell ID to be used as a center of k loop.
//      * @param k
//      *   Distance of k loop to be generated around the input cell ID.
//      * @return
//      *   A collection of cell IDs forming a k loop.
//      */
//    def kLoop(cellID: Long, k: Int): Seq[Long] = {
//        assert(k >= 1, "k must be at least 1")
//        val ring = kRing(cellID, k)
//        val innerRing = kRing(cellID, k - 1)
//        ring.diff(innerRing)
//    }
//
//    /**
//      * Returns the set of supported resolutions for the given grid system.
//      * This doesnt have to be a continuous set of values. Only values provided
//      * in this set are considered valid.
//      *
//      * @return
//      *   A set of supported resolutions.
//      */
//    def resolutions: Set[Int] = (0 to conf.maxResolution).toSet
//
//    /**
//      * Returns the resolution value based on the nullSafeEval method inputs of
//      * type Any. Each Grid System should ensure that only valid values of
//      * resolution are accepted.
//      *
//      * @param res
//      *   Any type input to be parsed into the Int representation of resolution.
//      * @return
//      *   Int value representing the resolution.
//      */
//    def getResolution(res: Any): Int = {
//        (
//          Try(res.asInstanceOf[Int]),
//          Try(res.asInstanceOf[String].toInt),
//          Try(res.asInstanceOf[UTF8String].toString.toInt)
//        ) match {
//            case (Success(value), _, _) if resolutions.contains(value) => value
//            case (_, Success(value), _) if resolutions.contains(value) => value
//            case (_, _, Success(value)) if resolutions.contains(value) => value
//            case _                                                     => throw new IllegalStateException(s"Resolution not supported: $res")
//        }
//    }
//
//    /**
//      * Computes the radius of minimum enclosing circle of the polygon
//      * corresponding to the centroid cell of the provided geometry.
//      *
//      * @param geometry
//      *   An instance of [[Geometry]] for which we are computing the optimal
//      *   buffer radius.
//      * @param resolution
//      *   A resolution to be used to get the centroid cell geometry.
//      * @return
//      *   An optimal radius to buffer the geometry in order to avoid blind spots
//      *   when performing polyfill.
//      */
//    def getBufferRadius(geometry: Geometry, resolution: Int): Double = {
//        // TODO: This is a very naive implementation, it should be improved
//        // Does not take into account the actual geometry, just the resolution
//        math.sqrt(math.pow(getCellWidth(resolution), 2) + math.pow(getCellHeight(resolution), 2)) / 2
//    }
//
//    /**
//      * Returns a set of indices that represent the input geometry. Depending on
//      * the grid system this set may include only indices whose centroids fall
//      * inside the input geometry or any cell that intersects the input
//      * geometry. When extending make sure which is the guaranteed behavior of
//      * the grid system.
//      *
//      * @param geometry
//      *   Input geometry to be represented.
//      * @param resolution
//      *   A resolution of the indices.
//      * @return
//      *   A set of indices representing the input geometry.
//      */
//    def polyfill(geometry: Geometry, resolution: Int): Seq[Long] = {
//        if (geometry.isEmpty) {
//            return Seq[Long]()
//        }
//        val envelope = geometry.getEnvelopeInternal
//        val minX = envelope.getMinX
//        val maxX = envelope.getMaxX
//        val minY = envelope.getMinY
//        val maxY = envelope.getMaxY
//
//        val (firstCellPosX, firstCellPosY, _) = getCellPositionFromCoordinates(minX, minY, resolution)
//        val (lastCellPosX, lastCellPosY, _) = getCellPositionFromCoordinates(maxX, maxY, resolution)
//
//        val cellCenters = (firstCellPosX to lastCellPosX + 1)
//            // Get all cells that overlap with the bounding box
//            .flatMap(x => (firstCellPosY to lastCellPosY + 1).map(y => (x, y)))
//
//            // Map them to cell centers and cell ID
//            .map(pos =>
//                (
//                  getCellCenterX(pos._1, resolution),
//                  getCellCenterY(pos._2, resolution)
//                )
//            )
//
//        val result = cellCenters
//            // Select only cells which center falls within the geometry
//            .filter(cell => geometry.contains(JTS.point(cell._1, cell._2)))
//
//            // Extract cellIDs only
//            .map(cell => pointToCellID(cell._1, cell._2, resolution))
//
//        result
//    }
//
//    def getCellResolution(cellId: Long): Int = {
//        (cellId >> conf.idBits).toInt
//    }
//
//    def getCellPosition(cellId: Long): Long = {
//        cellId & 0x00ffffffffffffffL
//    }
//
//    def getCellPositionX(idNumber: Long, resolution: Int): Long = {
//        idNumber % totalCellsX(resolution)
//    }
//
//    def getCellPositionY(idNumber: Long, resolution: Int): Long = {
//        Math.floor(idNumber / totalCellsX(resolution)).toLong
//    }
//
//    def getCellWidth(resolution: Int): Double = {
//        conf.rootCellSizeX / math.pow(conf.cellSplits, resolution)
//    }
//
//    def getCellHeight(resolution: Int): Double = {
//        conf.rootCellSizeY / math.pow(conf.cellSplits, resolution)
//    }
//
//    /**
//      * Get the geometry corresponding to the cell ID with the input id.
//      *
//      * @param cellID
//      *   Id of the cell whose geometry should be returned.
//      * @return
//      *   An instance of [[Geometry]] corresponding to cell ID.
//      */
//    // noinspection DuplicatedCode
//    def cellIdToGeometry(cellID: Long): Geometry = {
//
//        val cellNumber = getCellPosition(cellID)
//        val resolution = getCellResolution(cellID)
//        val cellX = getCellPositionX(cellNumber, resolution)
//        val cellY = getCellPositionY(cellNumber, resolution)
//
//        val edgeSizeX = getCellWidth(resolution)
//        val edgeSizeY = getCellHeight(resolution)
//
//        val x = cellX * edgeSizeX + conf.boundXMin
//        val y = cellY * edgeSizeY + conf.boundYMin
//
//        JTS.polygonFromXYs(
//          Array(
//            (x, y),
//            (x + edgeSizeX, y),
//            (x + edgeSizeX, y + edgeSizeY),
//            (x, y + edgeSizeY),
//            (x, y)
//          )
//        )
//    }
//
//    /**
//      * Get the cell ID corresponding to the provided coordinates.
//      *
//      * @param x
//      *   X coordinate of the point.
//      * @param y
//      *   Y coordinate of the point.
//      * @param resolution
//      *   Resolution of the grid.
//      * @return
//      *   Cell ID in this grid system.
//      */
//    def pointToCellID(x: Double, y: Double, resolution: Int): Long = {
//        require(!x.isNaN && !x.isNaN, throw new IllegalStateException("NaN coordinates are not supported."))
//        require(
//          resolution <= conf.maxResolution,
//          throw new IllegalStateException(s"Resolution exceeds maximum resolution of ${conf.maxResolution}.")
//        )
//        require(
//          x >= conf.boundXMin && x < conf.boundXMax,
//          throw new IllegalStateException(s"X coordinate ($x) out of bounds ${conf.boundXMin}-${conf.boundXMax}")
//        )
//        require(
//          y >= conf.boundYMin && y < conf.boundYMax,
//          throw new IllegalStateException(s"Y coordinate ($y) out of bounds ${conf.boundYMin}-${conf.boundYMax}")
//        )
//
//        val (_, _, cellPos) = getCellPositionFromCoordinates(x, y, resolution)
//        getCellId(cellPos, resolution)
//    }
//
//    def getCellPositionFromCoordinates(x: Double, y: Double, resolution: Int): (Long, Long, Long) = {
//        val cellPosX = ((x - conf.boundXMin) / getCellWidth(resolution)).toLong
//        val cellPosY = ((y - conf.boundYMin) / getCellHeight(resolution)).toLong
//        (cellPosX, cellPosY, getCellPositionFromPositions(cellPosX, cellPosY, resolution))
//    }
//
//    def totalCellsX(resolution: Int): Long = {
//        conf.rootCellCountX * Math.pow(conf.cellSplits, resolution).toLong
//    }
//
//    def totalCellsY(resolution: Int): Long = {
//        conf.rootCellCountY * Math.pow(conf.cellSplits, resolution).toLong
//    }
//
//    def distance(cellId: Long, cellId2: Long): Long = {
//        val resolution1 = getCellResolution(cellId)
//        val resolution2 = getCellResolution(cellId2)
//        val edgeSizeX = getCellWidth(resolution1)
//        val edgeSizeY = getCellHeight(resolution1)
//        val x1 = getCellCenterX(getCellPositionX(cellId, resolution1), resolution1)
//        val x2 = getCellCenterX(getCellPositionX(cellId2, resolution2), resolution2)
//        val y1 = getCellCenterY(getCellPositionY(cellId, resolution1), resolution1)
//        val y2 = getCellCenterY(getCellPositionY(cellId2, resolution2), resolution2)
//        // Manhattan distance with edge size precision
//        val distance = math.abs((x1 - x2) / edgeSizeX) + math.abs((y1 - y2) / edgeSizeY)
//        distance.toLong
//    }
//
//    private def getCellCenterX(cellPositionX: Long, resolution: Int) = {
//        val cellWidth = getCellWidth(resolution)
//
//        val centerOffset = cellPositionX * cellWidth + (cellWidth / 2)
//        centerOffset + conf.boundXMin
//    }
//
//    private def getCellCenterY(cellPositionY: Long, resolution: Int) = {
//        val cellHeight = getCellHeight(resolution)
//
//        val centerOffset = cellPositionY * cellHeight + (cellHeight / 2)
//        centerOffset + conf.boundYMin
//    }
//
//    private def getCellId(cellPosition: Long, resolution: Int) = {
//        val resBits = resolution.toLong << conf.idBits
//        val res = cellPosition | resBits
//
//        res
//    }
//
//    private def getCellPositionFromPositions(cellPosX: Long, cellPosY: Long, resolution: Int) = {
//        val cellsX = totalCellsX(resolution)
//        val cellPos = cellPosY * cellsX + cellPosX
//        cellPos
//    }
//
//    def cellIdToBoundary(cellID: Long): Seq[Coordinate] = {
//        val geometry = cellIdToGeometry(cellID)
//        if (geometry.isEmpty) {
//            Seq.empty[Coordinate]
//        } else {
//            geometry.getCoordinates.toSeq
//        }
//    }
//
//    def cellIdToCenter(cellID: Long): Coordinate = {
//        val geometry = cellIdToGeometry(cellID)
//        if (geometry.isEmpty) {
//            throw new IllegalStateException(s"Cell ID $cellID does not correspond to a valid geometry.")
//        }
//        geometry.getCentroid.getCoordinate
//    }
//
//}
