package com.databricks.labs.gbx.rasterx.expressions.grid

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.gridx.grid.H3
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

import scala.collection.mutable

object RST_H3_RasterToGrid {

    def cellPixel(gt: Array[Double], x: Int, y: Int, resolution: Int): Long = {
        val offset = 0.5 // This centers the point to the pixel centroid
        val xOffset = offset + x
        val yOffset = offset + y
        val xGeo = gt(0) + xOffset * gt(1) + yOffset * gt(2)
        val yGeo = gt(3) + xOffset * gt(4) + yOffset * gt(5)
        val cellID = H3.pointToCellID(xGeo, yGeo, resolution)
        cellID
    }

    def execute[T](
        ds: Dataset,
        resolution: Int,
        fAgg: mutable.ArrayBuffer[Double] => T
    ): Array[Array[(Long, T)]] = {

        val gt = ds.GetGeoTransform
        val xSize = ds.getRasterXSize
        val ySize = ds.getRasterYSize
        val nPix = xSize * ySize
        val bands = ds.getRasterCount

        val bandBuf = new Array[Double](nPix)
        val maskBuf = new Array[Byte](nPix)

        (1 to bands).iterator.map { bi =>
            val b = ds.GetRasterBand(bi)
            val m = b.GetMaskBand()
            b.ReadRaster(0, 0, xSize, ySize, bandBuf)
            m.ReadRaster(0, 0, xSize, ySize, maskBuf)

            var valid = 0; var i = 0
            while (i < nPix) { if (maskBuf(i) != 0) valid += 1; i += 1 }

            val acc = new mutable.LongMap[mutable.ArrayBuffer[Double]](valid)
            var y = 0; var idx = 0
            while (y < ySize) {
                var x = 0
                while (x < xSize) {
                    if (maskBuf(idx) != 0) {
                        val h = cellPixel(gt, x, y, resolution)
                        val buf = acc.getOrElseUpdate(h, new mutable.ArrayBuffer)
                        buf += bandBuf(idx)
                    }
                    idx += 1; x += 1
                }
                y += 1
            }

            // finalize: apply agg and emit (Long, Double)
            val out = new Array[(Long, T)](acc.size)
            var j = 0
            acc.foreach { case (cell, buf) => out(j) = (cell, fAgg(buf)); j += 1 }
            out
        }.toArray
    }

    def eval[T](
        row: InternalRow,
        resolution: Int,
        conf: UTF8String,
        rdt: DataType,
        execute: (Dataset, Int) => Array[Array[(Long, T)]]
    ): ArrayData = {
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        val ds = RasterSerializationUtil.rowToDS(row, rdt)
        val result = execute(ds, resolution)
        RasterDriver.releaseDataset(ds)
        ArrayData.toArrayData(
          result.map(band =>
              ArrayData.toArrayData(
                band.map { case (cellId, measure) => InternalRow.fromSeq(Seq(cellId, measure)) }
              )
          )
        )
    }

}
