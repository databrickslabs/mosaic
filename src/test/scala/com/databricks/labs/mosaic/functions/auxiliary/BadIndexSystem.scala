package com.databricks.labs.mosaic.functions.auxiliary

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.index.{IndexSystem, IndexSystemID}
import org.apache.spark.sql.types.{ArrayType, BinaryType, DataType}

// Used for testing only
object BadIndexSystem extends IndexSystem {
    override def getResolutionStr(resolution: Int): String = throw new UnsupportedOperationException

    override def format(id: Long): String = throw new UnsupportedOperationException

    override def defaultDataTypeID: DataType = ArrayType(BinaryType)

    override def kRing(index: Long, n: Int): Seq[Long] = throw new UnsupportedOperationException

    override def kDisk(index: Long, n: Int): Seq[Long] = throw new UnsupportedOperationException

    override def resolutions: Set[Int] = throw new UnsupportedOperationException

    override def getIndexSystemID: IndexSystemID = throw new UnsupportedOperationException

    override def getResolution(res: Any): Int = throw new UnsupportedOperationException

    override def getBufferRadius(geometry: MosaicGeometry, resolution: Int, geometryAPI: GeometryAPI): Double = throw new UnsupportedOperationException

    override def polyfill(geometry: MosaicGeometry, resolution: Int, geometryAPI: Option[GeometryAPI]): Seq[Long] = throw new UnsupportedOperationException

    override def indexToGeometry(index: Long, geometryAPI: GeometryAPI): MosaicGeometry = throw new UnsupportedOperationException

    override def indexToGeometry(index: String, geometryAPI: GeometryAPI): MosaicGeometry = throw new UnsupportedOperationException

    override def pointToIndex(lon: Double, lat: Double, resolution: Int): Long = throw new UnsupportedOperationException

    override def GridCenterAsWKB(index: Long, geometryAPI: GeometryAPI): MosaicGeometry = throw new UnsupportedOperationException
}