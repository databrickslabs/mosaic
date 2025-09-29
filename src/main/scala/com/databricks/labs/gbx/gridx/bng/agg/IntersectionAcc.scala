package com.databricks.labs.gbx.gridx.bng.agg

import com.databricks.labs.gbx.vectorx.jts.JTS

import java.nio.ByteBuffer

final case class IntersectionAcc(
    var initialized: Boolean,
    var cellID: Long,
    // running intersection of boundary geoms; null means “no boundary seen yet”
    var boundaryWkb: Array[Byte]
) {

    @inline private def intersectBytes(a: Array[Byte], b: Array[Byte]): Array[Byte] = {
        val g1 = JTS.fromWKB(a)
        if (g1.isEmpty) return a
        val g2 = JTS.fromWKB(b)
        if (g2.isEmpty) return JTS.toWKB(g2)
        JTS.toWKB(g1.intersection(g2))
    }

    def update(id: Long, isCore: Boolean, wkb: Array[Byte]): IntersectionAcc = {
        if (!initialized) { initialized = true; cellID = id }
        else require(cellID == id, "can only intersect chips based on the same grid cell")

        if (!isCore) {
            boundaryWkb =
                if (boundaryWkb eq null) java.util.Arrays.copyOf(wkb, wkb.length)
                else {
                    val out = intersectBytes(boundaryWkb, wkb)
                    boundaryWkb = out
                    out
                }
        }
        this
    }

    def merge(other: IntersectionAcc): IntersectionAcc = {
        if (!initialized) return other
        if (!other.initialized) return this
        require(cellID == other.cellID, "can only intersect chips based on the same grid cell")
        (boundaryWkb, other.boundaryWkb) match {
            case (null, null) => // all-core on both sides
            case (null, rb)   => boundaryWkb = rb // this had only cores
            case (_, null)    => // other had only cores
            case (lb, rb)     => boundaryWkb = intersectBytes(lb, rb)
        }
        this
    }

    // Serde: [initialized(1)][cellID(8)][hasBoundary(1)][len(4)?][wkb?]
    def serialize: Array[Byte] = {
        val hasB = boundaryWkb ne null
        val len = if (hasB) boundaryWkb.length else 0
        val bb = ByteBuffer.allocate(1 + 8 + 1 + (if (hasB) 4 + len else 0))
        bb.put(if (initialized) 1.toByte else 0.toByte)
        bb.putLong(cellID)
        bb.put(if (hasB) 1.toByte else 0.toByte)
        if (hasB) { bb.putInt(len); bb.put(boundaryWkb) }
        bb.array()
    }

}

object IntersectionAcc {

    val empty: IntersectionAcc = IntersectionAcc(initialized = false, 0L, null)
    def deserialize(bytes: Array[Byte]): IntersectionAcc = {
        val bb = ByteBuffer.wrap(bytes)
        val init = bb.get() != 0
        val id = bb.getLong
        val hasB = bb.get() != 0
        val wkb =
            if (hasB) { val n = bb.getInt; val a = new Array[Byte](n); bb.get(a); a }
            else null
        IntersectionAcc(init, id, wkb)
    }

}
