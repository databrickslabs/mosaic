package com.databricks.labs.mosaic.functions.auxiliary

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.types.model.Coordinates
import com.databricks.labs.mosaic.core.index.{IndexSystem, IndexSystemFactory}

import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, DataType}

// Used for testing only
object BadIndexSystem extends IndexSystem(BooleanType) {

    override def crsID: Int = throw new UnsupportedOperationException

    val name = "BadIndexSystem"

    override def getResolutionStr(resolution: Int): String = throw new UnsupportedOperationException

    override def format(id: Long): String = throw new UnsupportedOperationException

    override def kRing(index: Long, n: Int): Seq[Long] = throw new UnsupportedOperationException

    override def kLoop(index: Long, n: Int): Seq[Long] = throw new UnsupportedOperationException

    override def resolutions: Set[Int] = throw new UnsupportedOperationException

    override def getResolution(res: Any): Int = throw new UnsupportedOperationException

    override def getBufferRadius(geometry: MosaicGeometry, resolution: Int, geometryAPI: GeometryAPI): Double =
        throw new UnsupportedOperationException

    override def polyfill(geometry: MosaicGeometry, resolution: Int, geometryAPI: GeometryAPI): Seq[Long] =
        throw new UnsupportedOperationException

    override def indexToGeometry(index: Long, geometryAPI: GeometryAPI): MosaicGeometry = throw new UnsupportedOperationException

    override def pointToIndex(lon: Double, lat: Double, resolution: Int): Long = throw new UnsupportedOperationException

    override def parse(id: String): Long = throw new UnsupportedOperationException

    override def indexToBoundary(index: Long): Seq[Coordinates] = throw new UnsupportedOperationException

    override def indexToCenter(index: Long): Coordinates = throw new UnsupportedOperationException

    override def distance(cellId: Long, cellId2: Long): Long = throw new UnsupportedOperationException
}

