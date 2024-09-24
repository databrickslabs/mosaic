package com.databricks.labs.mosaic.core.raster.gdal

case class Padding(
    left: Boolean,
    right: Boolean,
    top: Boolean,
    bottom: Boolean
) {

    def removePadding(array: Array[Double], rowWidth: Int, stride: Int): Array[Double] = {
        val l = if (left) 1 else 0
        val r = if (right) 1 else 0
        val t = if (top) 1 else 0
        val b = if (bottom) 1 else 0
        
        val yStart = t * stride * rowWidth
        val yEnd = array.length - b * stride * rowWidth

        val slices = for (i <- yStart until yEnd by rowWidth) yield {
            val xStart = i + l * stride
            val xEnd = i + rowWidth - r * stride
            array.slice(xStart, xEnd)
        }

        slices.flatten.toArray
    }

    def horizontalStrides: Int = {
        if (left && right) 2
        else if (left || right) 1
        else 0
    }

    def verticalStrides: Int = {
        if (top && bottom) 2
        else if (top || bottom) 1
        else 0
    }

    def newOffset(xOffset: Int, yOffset: Int, stride: Int): (Int, Int) = {
        val x = if (left) xOffset + stride else xOffset
        val y = if (top) yOffset + stride else yOffset
        (x, y)
    }

    def newSize(width: Int, height: Int, stride: Int): (Int, Int) = {
        val w = if (left && right) width - 2 * stride else if (left || right) width - stride else width
        val h = if (top && bottom) height - 2 * stride else if (top || bottom) height - stride else height
        (w, h)
    }

}

object Padding {

    val NoPadding: Padding = Padding(left = false, right = false, top = false, bottom = false)

}
