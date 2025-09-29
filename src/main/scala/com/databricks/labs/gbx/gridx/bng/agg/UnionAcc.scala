package com.databricks.labs.gbx.gridx.bng.agg

import com.databricks.labs.gbx.vectorx.jts.JTS

import java.nio.ByteBuffer

final case class UnionAcc(
    var initialized: Boolean,
    var cellID: Long,
    var hasCore: Boolean,
    var unionWkb: Array[Byte] // running union of boundaries; null = none seen yet
) {

    @inline private def copyBytes(a: Array[Byte]) = java.util.Arrays.copyOf(a, a.length)
    @inline private def unionBytes(a: Array[Byte], b: Array[Byte]): Array[Byte] = {
        if (a eq null) return copyBytes(b)
        if (b eq null) return copyBytes(a)
        val g1 = JTS.fromWKB(a)
        val g2 = JTS.fromWKB(b)
        if (g1.isEmpty) return copyBytes(b)
        if (g2.isEmpty) return copyBytes(a)
        JTS.toWKB(g1.union(g2))
    }

    def update(id: Long, isCore: Boolean, wkb: Array[Byte]): UnionAcc = {
        if (!initialized) { initialized = true; cellID = id }
        else require(cellID == id, "can only union chips from the same grid cell")
        if (hasCore) return this
        if (isCore) { hasCore = true; unionWkb = null; return this }
        unionWkb = if (unionWkb eq null) copyBytes(wkb) else unionBytes(unionWkb, wkb)
        this
    }

    def merge(other: UnionAcc): UnionAcc = {
        if (!initialized) return other
        if (!other.initialized) return this
        require(cellID == other.cellID, "can only union chips from the same grid cell")
        if (hasCore || other.hasCore) { hasCore = true; unionWkb = null; return this }
        unionWkb = (unionWkb, other.unionWkb) match {
            case (null, rb) => rb
            case (lb, null) => lb
            case (lb, rb)   => unionBytes(lb, rb)
        }
        this
    }

    // serde: [init(1)][id(8)][hasCore(1)][hasUnion(1)][len(4)?][wkb?]
    def serialize: Array[Byte] = {
        val hasU = unionWkb ne null
        val len = if (hasU) unionWkb.length else 0
        val bb = ByteBuffer.allocate(1 + 8 + 1 + 1 + (if (hasU) 4 + len else 0))
        bb.put(if (initialized) 1.toByte else 0.toByte)
        bb.putLong(cellID)
        bb.put(if (hasCore) 1.toByte else 0.toByte)
        bb.put(if (hasU) 1.toByte else 0.toByte)
        if (hasU) { bb.putInt(len); bb.put(unionWkb) }
        bb.array()
    }

}

object UnionAcc {

    val empty: UnionAcc = UnionAcc(initialized = false, 0L, hasCore = false, unionWkb = null)
    def deserialize(bytes: Array[Byte]): UnionAcc = {
        val bb = ByteBuffer.wrap(bytes)
        val init = bb.get() != 0
        val id = bb.getLong
        val core = bb.get() != 0
        val hasU = bb.get() != 0
        val wkb =
            if (hasU) { val n = bb.getInt; val a = new Array[Byte](n); bb.get(a); a }
            else null
        UnionAcc(init, id, core, wkb)
    }

}
