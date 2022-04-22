package com.databricks.labs.mosaic.expressions.format

import collection.JavaConverters._
import com.databricks.labs.mosaic.core.types.cdm._
import java.util.UUID
import scala.reflect.ClassTag
import ucar.{ma2, nc2}

class CDMParser(bytes: Array[Byte]) {

    val ncFile: nc2.NetcdfFile = nc2.NetcdfFiles.openInMemory(UUID.randomUUID.toString, bytes)

    val nc2variables: Array[nc2.Variable] = ncFile.getVariables.asScala.toArray
    val variableNames: Array[String] = nc2variables.map(_.getShortName)

    val permittedDataTypes: List[ma2.DataType] =
        List(
          ma2.DataType.BYTE,
          ma2.DataType.UBYTE,
          ma2.DataType.STRING,
          ma2.DataType.CHAR,
          ma2.DataType.INT,
          ma2.DataType.UINT,
          ma2.DataType.LONG,
          ma2.DataType.ULONG,
          ma2.DataType.FLOAT,
          ma2.DataType.DOUBLE
        )

    def attributes(v: String): Array[CDMAttribute] =
        ncFile
            .findVariable(v)
            .attributes
            .asScala
            .filter(a => permittedDataTypes.contains(a.getDataType))
            .map(a =>
                (a.getShortName, a.getDataType) match {
                    case (n: String, ma2.DataType.BYTE | ma2.DataType.UBYTE)   => CDMAttribute.fromTypedValue[Byte](n, a.getValue(0))
                    case (n: String, ma2.DataType.STRING | ma2.DataType.CHAR)  => CDMAttribute.fromTypedValue[String](n, a.getValue(0))
                    case (n: String, ma2.DataType.SHORT | ma2.DataType.USHORT) => CDMAttribute.fromTypedValue[Short](n, a.getValue(0))
                    case (n: String, ma2.DataType.INT | ma2.DataType.UINT)     => CDMAttribute.fromTypedValue[Int](n, a.getValue(0))
                    case (n: String, ma2.DataType.LONG | ma2.DataType.ULONG)   => CDMAttribute.fromTypedValue[Long](n, a.getValue(0))
                    case (n: String, ma2.DataType.FLOAT)                       => CDMAttribute.fromTypedValue[Float](n, a.getValue(0))
                    case (n: String, ma2.DataType.DOUBLE)                      => CDMAttribute.fromTypedValue[Double](n, a.getValue(0))
                }
            )
            .toArray

    def description: CDMStructure =
        CDMStructure(nc2variables.map(v => CDMVariableAttributes(v.getShortName, v.getRank, v.getDataType.name, attributes(v.getFullName))))

    def variable(v: String): nc2.Variable = ncFile.findVariable(v)

    def read[T: ClassTag](v: String): CDMArray = {
        variable(v).getRank match {
            case 1 => CDMArray1d[T](
                  variable(v).read.copyToNDJavaArray
                      .asInstanceOf[Array[T]]
                )
            case 2 => CDMArray2d[T](
                  variable(v).read.copyToNDJavaArray
                      .asInstanceOf[Array[Array[T]]]
                )
            case 3 => CDMArray3d[T](
                  variable(v).read.copyToNDJavaArray
                      .asInstanceOf[Array[Array[Array[T]]]]
                )
        }
    }

}
