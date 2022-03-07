package com.databricks.mosaic.core.types.model

import com.databricks.mosaic.core.geometry.MosaicGeometry

case class MosaicFrameRow(
                             geometry: MosaicGeometry,
                             index: Long,
                             chips: Array[Byte],
                             enclosed: Boolean
                         )