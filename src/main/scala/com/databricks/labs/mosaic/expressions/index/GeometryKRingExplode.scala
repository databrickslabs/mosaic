package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.{IndexSystem, IndexSystemID}
import com.databricks.labs.mosaic.core.types.{HexType, InternalGeometryType}
import com.databricks.labs.mosaic.core.Mosaic
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

case class GeometryKRingExplode(geom: Expression, resolution: Expression, k: Expression, indexSystemName: String, geometryAPIName: String)
    extends CollectionGenerator
      with Serializable
      with CodegenFallback {

    val indexSystem: IndexSystem = IndexSystemID.getIndexSystem(IndexSystemID(indexSystemName))
    val geometryAPI: GeometryAPI = GeometryAPI(geometryAPIName)

    override def position: Boolean = false

    override def inline: Boolean = false

    override def children: Seq[Expression] = Seq(geom, resolution, k)

    // noinspection DuplicatedCode
    override def checkInputDataTypes(): TypeCheckResult = {
        if (!Seq(BinaryType, StringType, HexType, InternalGeometryType).contains(geom.dataType)) {
            TypeCheckResult.TypeCheckFailure("Unsupported geom type.")
        } else if (!Seq(IntegerType, StringType).contains(resolution.dataType)) {
            TypeCheckResult.TypeCheckFailure("Unsupported resolution type.")
        } else if (!Seq(IntegerType).contains(k.dataType)) {
            TypeCheckResult.TypeCheckFailure("Unsupported k type.")
        } else {
            TypeCheckResult.TypeCheckSuccess
        }
    }

    override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
        val geometryRaw = geom.eval(input)
        val resolutionRaw = resolution.eval(input)
        val kRaw = k.eval(input)
        if (geometryRaw == null || resolutionRaw == null || kRaw == null) {
            Seq.empty
        } else {
            val geometryVal = geometryAPI.geometry(geometryRaw, geom.dataType)
            val resolutionVal = indexSystem.getResolution(resolutionRaw)
            val kVal = kRaw.asInstanceOf[Int]

            val chips = Mosaic.getChips(geometryVal, resolutionVal, keepCoreGeom = false, indexSystem, geometryAPI)
            val (coreChips, borderChips) = chips.partition(_.isCore)

            val coreIndices = coreChips.map(_.cellIdAsLong(indexSystem)).toSet
            val borderIndices = borderChips.map(_.cellIdAsLong(indexSystem))

            val borderKRing = borderIndices.flatMap(indexSystem.kRing(_, kVal)).toSet
            val kRing = coreIndices ++ borderKRing


            kRing.map(row => InternalRow.fromSeq(Seq(indexSystem.serializeCellId(row))))
        }
    }

    override def elementSchema: StructType = StructType(Seq(StructField("cellId", indexSystem.getCellIdDataType)))

    override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(newChildren(0), newChildren(1), newChildren(2))

}


