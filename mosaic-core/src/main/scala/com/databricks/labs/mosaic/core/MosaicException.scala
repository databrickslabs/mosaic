package com.databricks.labs.mosaic.core

object MosaicException {

  def GeometryEncodingNotSupported(supportedGeometryEncodings: Seq[String], suppliedGeometryEncoding: String): Exception =
    new Exception(
      s"This expression only supports geometries encoded as ${supportedGeometryEncodings.mkString(",")}." +
        s"$suppliedGeometryEncoding was supplied as input."
    )


}
