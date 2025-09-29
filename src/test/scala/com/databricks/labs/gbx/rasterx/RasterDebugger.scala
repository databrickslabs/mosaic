package com.databricks.labs.gbx.rasterx

import org.gdal.gdal.Dataset

object RasterDebugger {

    def printGrayGrid(ds: Dataset, cols: Int = 16, rows: Int = 16, bandIndex: Int = 1): Unit = {
        require(cols > 0 && rows > 0, "grid must be > 0")
        val b = ds.GetRasterBand(bandIndex)
        val W = ds.GetRasterXSize; val H = ds.GetRasterYSize
        val grid = Array.ofDim[Double](rows, cols)

        var gy = 0
        while (gy < rows) {
            val y0 = (gy * H) / rows; val y1 = ((gy + 1) * H) / rows; val hh = math.max(1, y1 - y0)
            var gx = 0
            while (gx < cols) {
                val x0 = (gx * W) / cols; val x1 = ((gx + 1) * W) / cols; val ww = math.max(1, x1 - x0)
                val buf = new Array[Float](ww * hh)
                b.ReadRaster(x0, y0, ww, hh, buf)
                var s = 0.0; var c = 0; var i = 0
                while (i < buf.length) { val v = buf(i); if (!java.lang.Float.isNaN(v)) { s += v; c += 1 }; i += 1 }
                grid(gy)(gx) = if (c == 0) Double.NaN else s / c
                gx += 1
            }
            gy += 1
        }

        val vals = grid.flatten.filter(java.lang.Double.isFinite)
        val (mn, mx) = if (vals.isEmpty) (0.0, 1.0) else (vals.min, vals.max)
        val denom = if (mx == mn) 1.0 else mx - mn

        val sb = new StringBuilder(rows * (cols * 2 + 1))
        gy = 0
        while (gy < rows) {
            var gx = 0
            while (gx < cols) {
                val v = grid(gy)(gx)
                if (!java.lang.Double.isFinite(v)) sb.append("  ")
                else {
                    // scale to 0..23 xterm gray levels
                    val level = (((v - mn) / denom) * 23).toInt.max(0).min(23)
                    sb.append(s"\u001B[48;5;${232 + level}m  \u001B[0m")
                }
                gx += 1
            }
            sb.append('\n'); gy += 1
        }
        print(sb.toString)
    }

    def printGrayGridDense(ds: Dataset, cols: Int = 32, rows: Int = 32, bandIndex: Int = 1): Unit = {
        require(cols > 0 && rows > 0)
        val b = ds.GetRasterBand(bandIndex)
        val W = ds.GetRasterXSize; val H = ds.GetRasterYSize
        val grid = Array.ofDim[Double](rows, cols)

        var gy = 0
        while (gy < rows) {
            val y0 = (gy * H) / rows; val y1 = ((gy + 1) * H) / rows; val hh = math.max(1, y1 - y0)
            var gx = 0
            while (gx < cols) {
                val x0 = (gx * W) / cols; val x1 = ((gx + 1) * W) / cols; val ww = math.max(1, x1 - x0)
                val buf = new Array[Float](ww * hh)
                b.ReadRaster(x0, y0, ww, hh, buf)
                var s = 0.0; var c = 0; var i = 0
                while (i < buf.length) { val v = buf(i); if (!java.lang.Float.isNaN(v)) { s += v; c += 1 }; i += 1 }
                grid(gy)(gx) = if (c == 0) Double.NaN else s / c
                gx += 1
            }
            gy += 1
        }

        val vals = grid.flatten.filter(java.lang.Double.isFinite)
        val (mn, mx) = if (vals.isEmpty) (0.0, 1.0) else (vals.min, vals.max)
        val scale = if (mx == mn) 1.0 else mx - mn
        def idx(v: Double): Int = if (!java.lang.Double.isFinite(v)) -1 else math.max(0, math.min(23, (((v - mn) / scale) * 23).toInt))

        val sb = new StringBuilder
        gy = 0
        while (gy < rows) {
            val top = grid(gy); val bottom = if (gy + 1 < rows) grid(gy + 1) else Array.fill(cols)(Double.NaN)
            var gx = 0
            while (gx < cols) {
                val it = idx(top(gx)); val ib = idx(bottom(gx))
                if (it == -1 && ib == -1) sb.append(' ')
                else {
                    val fg = if (it >= 0) s"\u001B[38;5;${232 + it}m" else ""
                    val bg = if (ib >= 0) s"\u001B[48;5;${232 + ib}m" else ""
                    sb.append(bg).append(fg).append('\u2580').append("\u001B[0m") // '▀' upper half block
                }
                gx += 1
            }
            sb.append('\n'); gy += 2
        }
        print(sb.toString)
    }

    def printBrailleGrid(ds: Dataset, cols: Int = 32, rows: Int = 16, bandIndex: Int = 1): Unit = {
        require(cols > 0 && rows > 0)
        val b = ds.GetRasterBand(bandIndex)
        val W = ds.GetRasterXSize; val H = ds.GetRasterYSize
        // braille bit weights by (sx,sy): (0..1, 0..3)
        val w = Array(Array(1, 2, 4, 64), Array(8, 16, 32, 128))

        val sb = new StringBuilder(rows * (cols + 1))
        var gy = 0
        while (gy < rows) {
            var gx = 0
            while (gx < cols) {
                // 2x4 subcells within this braille cell
                var sum = 0.0; var cnt = 0
                val sub = Array.ofDim[Double](4, 2)
                var sy = 0
                while (sy < 4) {
                    val y0 = ((gy * 4 + sy) * H) / (rows * 4); val y1 = ((gy * 4 + sy + 1) * H) / (rows * 4); val hh = math.max(1, y1 - y0)
                    var sx = 0
                    while (sx < 2) {
                        val x0 = ((gx * 2 + sx) * W) / (cols * 2); val x1 = ((gx * 2 + sx + 1) * W) / (cols * 2)
                        val ww = math.max(1, x1 - x0)
                        val buf = new Array[Float](ww * hh)
                        b.ReadRaster(x0, y0, ww, hh, buf)
                        var s = 0.0; var c = 0; var i = 0
                        while (i < buf.length) { val v = buf(i); if (!java.lang.Float.isNaN(v)) { s += v; c += 1 }; i += 1 }
                        val avg = if (c == 0) Double.NaN else s / c
                        sub(sy)(sx) = avg
                        if (!avg.isNaN) { sum += avg; cnt += 1 }
                        sx += 1
                    }
                    sy += 1
                }
                val thr = if (cnt == 0) Double.NaN else sum / cnt
                var mask = 0
                if (!thr.isNaN) {
                    var sy2 = 0
                    while (sy2 < 4) {
                        var sx2 = 0
                        while (sx2 < 2) {
                            val v = sub(sy2)(sx2)
                            if (java.lang.Double.isFinite(v) && v >= thr) mask |= w(sx2)(sy2)
                            sx2 += 1
                        }
                        sy2 += 1
                    }
                }
                sb.append((0x2800 + mask).toChar)
                gx += 1
            }
            sb.append('\n'); gy += 1
        }
        print(sb.toString)
    }

    def printColorGridDense256(ds: Dataset, cols: Int = 32, rows: Int = 32, bandIndex: Int = 1): Unit = {
        require(cols > 0 && rows > 0)
        val b = ds.GetRasterBand(bandIndex)
        val W = ds.GetRasterXSize; val H = ds.GetRasterYSize
        // 24-step xterm 256-color palette (blue cyan green yellow)
        val grid = Array.ofDim[Double](rows, cols)
        val palette: Array[Int] =
            Array(17, 18, 19, 20, 26, 27, 33, 34, 40, 41, 47, 48, 49, 84, 85, 86, 121, 122, 157, 158, 193, 194, 229, 230)
        @inline def c256(i: Int, fg: Boolean): String = if (i < 0) "" else s"\u001B[${if (fg) "38" else "48"};5;${palette(i)}m"
        @inline def reset: String = "\u001B[0m"

        var gy = 0
        while (gy < rows) {
            val y0 = (gy * H) / rows; val y1 = ((gy + 1) * H) / rows; val hh = math.max(1, y1 - y0)
            var gx = 0
            while (gx < cols) {
                val x0 = (gx * W) / cols; val x1 = ((gx + 1) * W) / cols; val ww = math.max(1, x1 - x0)
                val buf = new Array[Float](ww * hh)
                b.ReadRaster(x0, y0, ww, hh, buf)
                var s = 0.0; var c = 0; var i = 0
                while (i < buf.length) { val v = buf(i); if (!java.lang.Float.isNaN(v)) { s += v; c += 1 }; i += 1 }
                grid(gy)(gx) = if (c == 0) Double.NaN else s / c
                gx += 1
            }
            gy += 1
        }

        val vals = grid.flatten.filter(java.lang.Double.isFinite)
        val (mn, mx) = if (vals.isEmpty) (0.0, 1.0) else (vals.min, vals.max)
        val scale = if (mx == mn) 1.0 else mx - mn
        @inline def idx(v: Double): Int =
            if (!java.lang.Double.isFinite(v)) -1
            else math.max(0, math.min(palette.length - 1, (((v - mn) / scale) * (palette.length - 1)).toInt))

        val sb = new StringBuilder
        var y = 0
        while (y < rows) {
            val top = grid(y)
            val bottom = if (y + 1 < rows) grid(y + 1) else Array.fill(cols)(Double.NaN)
            var x = 0
            while (x < cols) {
                val it = idx(top(x)); val ib = idx(bottom(x))
                if (it == -1 && ib == -1) sb.append(' ')
                else {
                    val fg = c256(it, fg = true)
                    val bg = c256(ib, fg = false)
                    sb.append(bg).append(fg).append('\u2580').append(reset) // '▀'
                }
                x += 1
            }
            sb.append('\n')
            y += 2
        }
        print(sb.toString)
    }

    def printColorGridDenseTruecolor(ds: Dataset, cols: Int = 32, rows: Int = 32, bandIndex: Int = 1): Unit = {
        require(cols > 0 && rows > 0)
        val b = ds.GetRasterBand(bandIndex)
        val W = ds.GetRasterXSize; val H = ds.GetRasterYSize
        val grid = Array.ofDim[Double](rows, cols)

        var gy = 0
        while (gy < rows) {
            val y0 = (gy * H) / rows; val y1 = ((gy + 1) * H) / rows; val hh = math.max(1, y1 - y0)
            var gx = 0
            while (gx < cols) {
                val x0 = (gx * W) / cols; val x1 = ((gx + 1) * W) / cols; val ww = math.max(1, x1 - x0)
                val buf = new Array[Float](ww * hh)
                b.ReadRaster(x0, y0, ww, hh, buf)
                var s = 0.0; var c = 0; var i = 0
                while (i < buf.length) { val v = buf(i); if (!java.lang.Float.isNaN(v)) { s += v; c += 1 }; i += 1 }
                grid(gy)(gx) = if (c == 0) Double.NaN else s / c
                gx += 1
            }
            gy += 1
        }

        val vals = grid.flatten.filter(java.lang.Double.isFinite)
        val (mn, mx) = if (vals.isEmpty) (0.0, 1.0) else (vals.min, vals.max)
        val denom = math.max(1e-12, mx - mn)
        @inline def clamp01(x: Double) = if (x < 0) 0.0 else if (x > 1) 1.0 else x

        // Truecolor viridis-ish stops (purple blue teal green yellow)
        val stops = Array((68, 1, 84), (59, 82, 139), (33, 145, 140), (94, 201, 98), (253, 231, 37))
        @inline def cmap(t0: Double): (Int, Int, Int) = {
            val t = clamp01(t0); val n = stops.length; val p = t * (n - 1)
            val k = math.min(n - 2, math.max(0, p.floor.toInt)); val u = p - k
            val (r1, g1, b1) = stops(k); val (r2, g2, b2) = stops(k + 1)
            ((r1 + ((r2 - r1) * u)).toInt, (g1 + ((g2 - g1) * u)).toInt, (b1 + ((b2 - b1) * u)).toInt)
        }
        @inline def tOf(v: Double) = clamp01((v - mn) / denom)
        @inline def fg24(v: Double): String =
            if (!java.lang.Double.isFinite(v)) "" else { val (r, g, b) = cmap(tOf(v)); s"\u001B[38;2;$r;$g;${b}m" }
        @inline def bg24(v: Double): String =
            if (!java.lang.Double.isFinite(v)) "" else { val (r, g, b) = cmap(tOf(v)); s"\u001B[48;2;$r;$g;${b}m" }
        @inline def reset: String = "\u001B[0m"

        val sb = new StringBuilder
        var y = 0
        while (y < rows) {
            val top = grid(y)
            val bottom = if (y + 1 < rows) grid(y + 1) else Array.fill(cols)(Double.NaN)
            var x = 0
            while (x < cols) {
                val fg = fg24(top(x))
                val bg = bg24(bottom(x))
                if (fg.isEmpty && bg.isEmpty) sb.append(' ')
                else sb.append(bg).append(fg).append('\u2580').append(reset) // grayscale little square
                x += 1
            }
            sb.append('\n')
            y += 2
        }
        print(sb.toString)
    }

}
