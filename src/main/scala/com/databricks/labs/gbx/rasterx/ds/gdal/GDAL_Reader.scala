package com.databricks.labs.gbx.rasterx.ds.gdal

import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.BalancedSubdivision
import com.databricks.labs.gbx.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.unsafe.types.UTF8String

class GDAL_Reader(partition: GDAL_Partition) extends PartitionReader[InternalRow] {

    RST_ExpressionUtil.init(partition.expressionConfig)

    // TODO: Options should come from the partition here
    private val ds = RasterDriver.read(partition.filePath, Map.empty)
    private val tilesIter = BalancedSubdivision.splitRasterIter(ds, Map.empty, partition.sizeInMB)
    RST_ExpressionUtil.addCleanupListener(tilesIter)
    private var counter = 0
    private val hconf = partition.expressionConfig.hConf

    override def next(): Boolean = tilesIter.hasNext

    override def get(): InternalRow = {
        val tile = tilesIter.next()
        counter += 1
        val tileRow = RasterSerializationUtil.tileToRow((-1L, tile._1, tile._2), BinaryType, hconf)
        RasterDriver.releaseDataset(tile._1)
        InternalRow.fromSeq(
          Seq(
            UTF8String.fromString(partition.filePath),
            tileRow
          )
        )
    }

    override def close(): Unit = {
        // we close as we go, so nothing to do here
    }

}
