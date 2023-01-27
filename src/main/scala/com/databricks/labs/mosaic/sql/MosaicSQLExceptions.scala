package com.databricks.labs.mosaic.sql

import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum

object MosaicSQLExceptions {

    def NoGeometryColumnSet: Exception =
        new Exception(
          "Cannot index a MosaicFrame which does not have an active geometry column set. Use the setGeometryColumn method to update this value."
        )

    def NotEnoughGeometriesException: Exception =
        new Exception(
          "Not enough geometries supplied to MosaicAnalyser to compute resolution metrics. " +
              "Try increasing the sampleFraction using the setSampleFraction method."
        )

    def BadIndexResolution(resolutions: Set[Int]): Exception =
        new Exception(
          s"Resolution supplied to setIndexResolution must be in the set of values: $resolutions."
        )

    def MosaicFrameNotIndexed: Exception =
        new Exception(
          "Cannot perform this action before the MosaicFrame has been indexed. Use applyIndex() to index the MosaicFrame"
        )

    def SpatialJoinTypeNotSupported(geometryType: GeometryTypeEnum.Value, otherGeometryType: GeometryTypeEnum.Value): Exception =
        new Exception(
          s"Joins between $geometryType and $otherGeometryType are not supported with MosaicFrames."
        )

    def GeometryEncodingNotSupported(supportedGeometryEncodings: List[String], suppliedGeometryEncoding: String): Exception =
        new Exception(
          s"This expression only supports geometries encoded as ${supportedGeometryEncodings.mkString(",")}." +
              s"$suppliedGeometryEncoding was supplied as input."
        )

}
