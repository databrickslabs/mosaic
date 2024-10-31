package com.databricks.labs.mosaic.core.types.model

import java.util.Locale

object TriangulationSplitPointTypeEnum extends Enumeration {
    val MIDPOINT: TriangulationSplitPointTypeEnum.Value = Value("MIDPOINT")
    val NONENCROACHING: TriangulationSplitPointTypeEnum.Value = Value("NONENCROACHING")

    def fromString(value: String): TriangulationSplitPointTypeEnum.Value =
        TriangulationSplitPointTypeEnum.values
            .find(_.toString == value.toUpperCase(Locale.ROOT))
            .getOrElse(
              throw new Error(
                s"Invalid mode for triangulation split point type: $value." +
                    s" Must be one of ${TriangulationSplitPointTypeEnum.values.mkString(",")}"
              )
            )
}
