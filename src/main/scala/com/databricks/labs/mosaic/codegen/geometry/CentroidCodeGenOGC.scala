package com.databricks.labs.mosaic.codegen.geometry

import com.esri.core.geometry.ogc.OGCPoint

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext

object CentroidCodeGenOGC {
    // noinspection DuplicatedCode
    def centroid(ctx: CodegenContext, eval: String): (String, String) = {
        val centroid = ctx.freshName("centroid")
        val centroidValues = ctx.freshName("values")
        val centroidRow = ctx.freshName("centroidRow")
        val rowClass = classOf[GenericInternalRow].getName
        val ogcPointClass = classOf[OGCPoint].getName
        (
          s"""
             |$ogcPointClass $centroid = (($ogcPointClass) $eval.centroid());
             |Object[] $centroidValues;
             |if($centroid.is3D()) {
             |    $centroidValues = new Object[3];
             |    $centroidValues[0] = $centroid.X();
             |    $centroidValues[1] = $centroid.Y();
             |    $centroidValues[2] = $centroid.Z();
             |} else {
             |    $centroidValues = new Object[2];
             |    $centroidValues[0] = $centroid.X();
             |    $centroidValues[1] = $centroid.Y();
             |}
             |InternalRow $centroidRow = new $rowClass($centroidValues);
             |""".stripMargin,
          centroidRow
        )
    }

}
