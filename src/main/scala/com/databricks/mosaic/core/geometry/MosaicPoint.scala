package com.databricks.mosaic.core.geometry

import com.uber.h3core.util.GeoCoord

trait MosaicPoint {

  def getX: Double

  def getY: Double

  def getZ: Double

  def distance(other: MosaicPoint): Double

  def geoCoord: GeoCoord

}
