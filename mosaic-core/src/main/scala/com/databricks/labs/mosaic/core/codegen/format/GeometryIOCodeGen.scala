package com.databricks.labs.mosaic.core.codegen.format

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext

/**
 * GeometryIOCodeGen is a trait that defines the interface for generating code for the various geometry formats.
 * To support a new format toFormat and fromFormat methods need to be added to this trait.
 * This is the IO CodeGen contract for all Geometry implementations.
 */
trait GeometryIOCodeGen {

  def fromWKT(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String)

  def fromWKB(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String)

  def fromGeoJSON(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String)

  def fromHex(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String)

  def toWKT(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String)

  def toWKB(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String)

  def toGeoJSON(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String)

  def toHEX(ctx: CodegenContext, eval: String, geometryAPI: GeometryAPI): (String, String)

}
