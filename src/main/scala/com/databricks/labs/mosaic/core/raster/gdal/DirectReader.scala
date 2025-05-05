package com.databricks.labs.mosaic.core.raster.gdal

object DirectReader {

    def readWindow(band: MosaicRasterBandGDAL, window: (Int, Int, Int, Int)): Array[Array[Double]] = {
        val noData = band.noDataValue
        val (xStart, yStart, xEnd, yEnd) = window

        val minX = Math.min(xStart, xEnd)
        val maxX = Math.max(xStart, xEnd)
        val minY = Math.min(yStart, yEnd)
        val maxY = Math.max(yStart, yEnd)

        val xs = Math.max(0, minX)
        val xe = Math.min(maxX, band.xSize)
        val ys = Math.max(0, minY)
        val ye = Math.min(maxY, band.ySize)

        val w = xe - xs
        val h = ye - ys
        if (w <= 0 || h <= 0) return null

        val data = Array.ofDim[Double](w * h)
        val maskData = Array.ofDim[Byte](w * h)
        band.band.ReadRaster(xs, ys, w, h, data)
        band.band.GetMaskBand().ReadRaster(xs, ys, w, h, maskData)

        val masked = data.zip(maskData).map { case (d, m) => if (m == 0) noData else d }

        val width = Math.abs(xEnd - xStart)
        val height = Math.abs(yEnd - yStart)
        val buffer = Array.fill(height, width)(noData)

        val xOffset = xs - minX
        val yOffset = ys - minY
        for (i <- 0 until h) {
            val srcPos = i * w
            System.arraycopy(masked, srcPos, buffer(i + yOffset), xOffset, w)
        }

        buffer.find(_.exists(_ != noData)) match {
            case Some(_) => buffer
            case _       => null
        }
    }

}
