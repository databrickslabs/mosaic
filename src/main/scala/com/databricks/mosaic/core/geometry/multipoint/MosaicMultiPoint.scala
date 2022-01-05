package com.databricks.mosaic.core.geometry.multipoint

import com.databricks.mosaic.core.geometry.MosaicGeometry
import com.databricks.mosaic.core.geometry.point.MosaicPoint

trait MosaicMultiPoint extends MosaicGeometry {

  def asSeq: Seq[MosaicPoint]

}
