package com.databricks.mosaic.codegen.geometry

import org.locationtech.jts.geom.Point

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext

object CentroidCodeGenJTS {
    // noinspection DuplicatedCode
    def centroid(ctx: CodegenContext, eval: String, nDim: Int): (String, String) = {
        val centroid = ctx.freshName("centroid")
        val centroidValues = ctx.freshName("values")
        val centroidRow = ctx.freshName("centroidRow")
        val rowClass = classOf[GenericInternalRow].getName
        val jtsPointClass = classOf[Point].getName
        (
          s"""
             |$jtsPointClass $centroid = $eval.getCentroid();
             |Object[] $centroidValues;
             |if($nDim == 3) {
             |    $centroidValues = new Object[3];
             |    $centroidValues[0] = $centroid.getX();
             |    $centroidValues[1] = $centroid.getY();
             |    $centroidValues[2] = $centroid.getCoordinate().z;
             |} else {
             |    $centroidValues = new Object[2];
             |    $centroidValues[0] = $centroid.getX();
             |    $centroidValues[1] = $centroid.getY();
             |}
             |InternalRow $centroidRow = new $rowClass($centroidValues);
             |""".stripMargin,
          centroidRow
        )
    }

}
