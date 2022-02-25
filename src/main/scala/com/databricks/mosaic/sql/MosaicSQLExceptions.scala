package com.databricks.mosaic.sql

import com.databricks.mosaic.core.types.model.GeometryTypeEnum


object MosaicSQLExceptions {
  def NotEnoughGeometriesException: Exception = new Exception(
    "Not enough geometries supplied to MosaicAnalyser to compute resolution metrics. " +
      "Try increasing the sampleFraction using the setSampleFraction method.")

  def IncorrectGeometryTypeSupplied(simpleString: String, supplied: GeometryTypeEnum.Value, expected: GeometryTypeEnum.Value): Exception = new Exception(
    s"Could not execute $simpleString. Wrong geometry type supplied to function." +
      s"Expected $expected, received $supplied."
  )

  def NoIndexResolutionSet: Exception = new Exception(
    "No index resolutions set on this MosaicFrame. Use getOptimalResolution to determine what this value should be" +
      "and setIndexResolution to update this value"
  )

  def MosaicFrameNotIndexed: Exception = new Exception(
    "Cannot perform this action before the MosaicFrame has been indexed. Use applyIndex() to index the MosaicFrame"
  )

  def SpatialJoinTypeNotSupported(geometryType: GeometryTypeEnum.Value, otherGeometryType: GeometryTypeEnum.Value): Exception = new Exception(
    s"Joins between $geometryType and $otherGeometryType are not supported with MosaicFrames."
  )
}
