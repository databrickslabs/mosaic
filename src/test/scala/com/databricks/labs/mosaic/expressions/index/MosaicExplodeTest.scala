package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class MosaicExplodeTest extends MosaicSpatialQueryTest with SharedSparkSession with MosaicExplodeBehaviors {

    testAllNoCodegen("MosaicExplode WKT decompose") { wktDecompose }
    testAllNoCodegen("MosaicExplode WKT decompose keep core geoms") { wktDecomposeKeepCoreParamExpression }
    testAllNoCodegen("MosaicExplode WKT decompose no nulls") { wktDecomposeNoNulls }
    testAllNoCodegen("MosaicExplode WKB decompose") { wkbDecompose }
    testAllNoCodegen("MosaicExplode Hex decompose") { hexDecompose }
    testAllNoCodegen("MosaicExplode Coords decompose") { coordsDecompose }
    testAllNoCodegen("MosaicExplode Line decompose") { lineDecompose }
    testAllNoCodegen("MosaicExplode Line decompose first point on boundary") { lineDecomposeFirstPointOnBoundary }
    testAllNoCodegen("MosaicExplode column function signatures") { columnFunctionSignatures }
    testAllNoCodegen("MosaicExplode auxiliary methods") { auxiliaryMethods }

}
