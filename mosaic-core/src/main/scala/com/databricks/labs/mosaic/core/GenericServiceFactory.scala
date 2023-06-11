package com.databricks.labs.mosaic.core

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.raster.RasterAPI
import com.databricks.labs.mosaic.core.util.ResourceUtils

import scala.util.Try

/**
 * Generic service factory for loading implementations of [[com.databricks.labs.mosaic.core.geometry.api.GeometryAPI]],
 * [[com.databricks.labs.mosaic.core.index.IndexSystem]] and [[com.databricks.labs.mosaic.core.raster.RasterAPI]].
 * This class implements the interaction with the META-INF/services directory.
 * All the implementations are provided via the META-INF/services directory and are loaded at runtime.
 */
abstract class GenericServiceFactory[T](registerName: String) {

  private def fetchClasses: Seq[Class[_]] = {
    ResourceUtils.readResourceLines(s"/META-INF/services/$registerName")
      .map(name => Try(Class.forName(name)))
      .filter(_.isSuccess)
      .map(_.get)
      .toSeq
  }

  def getService(name: String, params: Array[Object] = Array.empty): T = {
    val classes = fetchClasses
    val instance = classes
      .map(clazz => Try(clazz.getConstructor(params.map(_.getClass): _*)))
      .map(_.map(_.newInstance(params: _*).asInstanceOf[T]))
      .filter(_.isSuccess)
      .map(_.get)
      .headOption

    instance.getOrElse(
      throw new IllegalArgumentException(s"Unable to find service with name $name")
    )
  }

}

/**
 * This object contains the actual factory instances for [[com.databricks.labs.mosaic.core.geometry.api.GeometryAPI]],
 * [[com.databricks.labs.mosaic.core.index.IndexSystem]] and [[com.databricks.labs.mosaic.core.raster.RasterAPI]].
 */
object GenericServiceFactory {

  object GeometryAPIFactory
    extends GenericServiceFactory[GeometryAPI](registerName = "com.databricks.labs.mosaic.GeometryAPIRegister") {
    def getGeometryAPI(name: String, params: Array[Object] = Array.empty): GeometryAPI = {
      getService(name, params)
    }
  }

  object IndexSystemFactory
    extends GenericServiceFactory[IndexSystem](registerName = "com.databricks.labs.mosaic.IndexSystemRegister") {
    def getIndexSystem(name: String, params: Array[Object] = Array.empty): IndexSystem = {
      getService(name, params)
    }
  }

  object RasterAPIFactory
    extends GenericServiceFactory[RasterAPI](registerName = "com.databricks.labs.mosaic.RasterAPIRegister") {
    def getRasterAPI(name: String, params: Array[Object] = Array.empty): RasterAPI = {
      getService(name, params)
    }
  }

}
