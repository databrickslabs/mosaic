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

    def IncorrectGeometryTypeSupplied(simpleString: String, supplied: GeometryTypeEnum.Value, expected: GeometryTypeEnum.Value): Exception =
        new Exception(
          s"Could not execute $simpleString. Wrong geometry type supplied to function." +
              s"Expected $expected, received $supplied."
        )

    def NoIndexResolutionSet: Exception =
        new Exception(
          "No index resolutions set on this MosaicFrame. Use getOptimalResolution to determine what this value should be" +
              "and setIndexResolution to update this value"
        )

    def BadIndexResolution(resMin: Int, resMax: Int): Exception =
        new Exception(
          s"Resolution supplied to setIndexResolution must be between $resMin and $resMax inclusive."
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
