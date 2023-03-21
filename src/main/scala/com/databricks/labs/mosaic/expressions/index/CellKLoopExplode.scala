package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.{IndexSystem, IndexSystemFactory}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

case class CellKLoopExplode(cellId: Expression, k: Expression, indexSystem: IndexSystem, geometryAPIName: String)
    extends CollectionGenerator
      with Serializable
      with CodegenFallback {

    val geometryAPI: GeometryAPI = GeometryAPI(geometryAPIName)

    override def position: Boolean = false

    override def inline: Boolean = false

    override def children: Seq[Expression] = Seq(cellId, k)

    // noinspection DuplicatedCode
    override def checkInputDataTypes(): TypeCheckResult = {
        if (!Seq(LongType, StringType).contains(cellId.dataType)) {
            TypeCheckResult.TypeCheckFailure("Unsupported cell ID type.")
        } else if (!Seq(IntegerType).contains(k.dataType)) {
            TypeCheckResult.TypeCheckFailure("Unsupported k type.")
        } else {
            TypeCheckResult.TypeCheckSuccess
        }
    }

    override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
        val cellIdValue = cellId.eval(input)
        val kValue = k.eval(input)
        if (cellIdValue == null || kValue == null) {
            Seq.empty
        } else {
            val cellId = indexSystem.formatCellId(cellIdValue, LongType).asInstanceOf[Long]
            val indices = indexSystem.kLoop(cellId, kValue.asInstanceOf[Int])
            indices.map(row => InternalRow.fromSeq(Seq(indexSystem.serializeCellId(row))))
        }
    }

    override def elementSchema: StructType = StructType(Seq(StructField("cellId", indexSystem.getCellIdDataType)))

    override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(newChildren(0), newChildren(1))

}

object CellKLoopExplode {

    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[CellKLoopExplode].getCanonicalName,
          db.orNull,
          "grid_cellkloopexplode",
          """
            |    _FUNC_(cell_id, resolution)) - Generates the cell based k loop (hollow ring) cell IDs set for the input
            |    cell ID and the input k value.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a, b);
            |        622236721274716159
            |        622236721274716160
            |        622236721274716161
            |        ...
            |
            |  """.stripMargin,
          "",
          "generator_funcs",
          "1.0",
          "",
          "built-in"
        )
}
